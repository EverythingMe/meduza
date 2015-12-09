package schema

import (
	"github.com/dvirsky/go-pylog/logging"

	"unicode"

	"github.com/EverythingMe/meduza/errors"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"

	"golang.org/x/text/transform"
)

type TextNormalizer interface {
	Normalize([]byte) (string, error)
	NormalizeString(string) (string, error)
}

type DefaultTextNormalizer struct {
	Locale      language.Tag
	transformer transform.Transformer
}

func filterMn(r rune) bool {
	return unicode.Is(unicode.Mn, r)
}

func filterPunct(r rune) bool {
	return unicode.Is(unicode.Punct, r)
}

func defaultLocale() language.Tag {
	l, e := language.Parse("icu")
	if e != nil {
		return language.Und
	}
	return l
}

// A utilit to deduplicate consecutive whitespaces from text values
type whitespaceDedupe struct{}

func (whitespaceDedupe) isWhite(b byte) bool {
	switch b {
	case '\t', '\n', '\v', '\f', '\r', ' ':
		return true
	}
	return false
}

// Transform takes src and copies to dst only bytes that are not whitespaces AFTER whitespaces
func (dd whitespaceDedupe) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
	d := 0

	for i, b := range src {

		if i >= len(dst) {
			return d, i, transform.ErrShortDst
		}

		if dd.isWhite(b) && i > 0 && dd.isWhite(src[i-1]) {
			continue
		}

		dst[d] = b
		d++

	}
	return d, len(src), nil
}

func (whitespaceDedupe) Reset() {}

func NewNormalizer(locale language.Tag, removeAccents, removePunct bool) *DefaultTextNormalizer {

	if locale == language.Und {
		locale = defaultLocale()
	}

	chain := []transform.Transformer{norm.NFD}

	if removeAccents {
		chain = append(chain, transform.RemoveFunc(filterMn))
	}
	if removePunct {
		chain = append(chain, transform.RemoveFunc(filterPunct))
	}

	chain = append(chain, cases.Lower(locale), whitespaceDedupe{})

	return &DefaultTextNormalizer{
		Locale:      locale,
		transformer: transform.Chain(chain...),
	}

}

func (d *DefaultTextNormalizer) Normalize(input []byte) (ret string, err error) {

	defer func() {
		e := recover()
		if e != nil {
			err = errors.NewError("PANIC in normalization: %s", e)
			logging.Error("Error while trying to normalize string '%s' (%v) - %v", string(input), input, e)
		}
	}()

	if input == nil || len(input) == 0 {
		ret = ""
		return
	}
	var b []byte
	b, _, err = transform.Bytes(d.transformer, input)
	if err != nil {
		logging.Warning("Error transforming string '%s': %s", string(input), err)
		err = errors.NewError("Error transforming string: %s", err)
		return
	}
	ret = string(b)
	return
}

func (d *DefaultTextNormalizer) NormalizeString(input string) (string, error) {
	return d.Normalize([]byte(input))
}

type Tokenizer interface {
	Tokenize(string) error
	HasNext()
	NextToken() string
}
