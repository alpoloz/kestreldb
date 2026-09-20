package engine

import "testing"

func TestGlobMatch(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		want    bool
	}{
		{pattern: "*", value: "anything/including/slashes", want: true},
		{pattern: "user:*", value: "user:42", want: true},
		{pattern: "user:?", value: "user:42", want: false},
		{pattern: "file[0-9]", value: "file7", want: true},
		{pattern: "file[^0-9]", value: "filex", want: true},
		{pattern: `literal\*`, value: "literal*", want: true},
		{pattern: "a**b", value: "axyzb", want: true},
		{pattern: "[z-a]", value: "m", want: true},
		{pattern: "[abc", value: "[abc", want: true},
	}

	for _, test := range tests {
		t.Run(test.pattern+"/"+test.value, func(t *testing.T) {
			if got := globMatch(test.pattern, test.value); got != test.want {
				t.Fatalf("globMatch(%q, %q) = %t, want %t", test.pattern, test.value, got, test.want)
			}
		})
	}
}
