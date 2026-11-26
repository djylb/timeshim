package timeshim_test

import (
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// TestStandardLibraryOnly checks that every package, tests included,
// depends on the standard library and this module alone.
func TestStandardLibraryOnly(t *testing.T) {
	if runtime.GOOS == "js" || runtime.GOOS == "wasip1" {
		t.Skip("cannot run the go command")
	}
	goCmd, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go command")
	}
	const format = "{{if not .Standard}}{{.ImportPath}}\t{{with .Module}}{{.Main}}\t{{.Path}}{{end}}{{end}}"
	out, err := exec.CommandContext(t.Context(), goCmd, "list", "-deps", "-test", "-f", format, "./...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for line := range strings.Lines(string(out)) {
		path, owner, ok := strings.Cut(strings.TrimSpace(line), "\t")
		if path != "" && (!ok || !ownModule(owner)) {
			t.Errorf("dependency outside the standard library: %s", path)
		}
	}
}

const modulePath = "github.com/djylb/timeshim"

func ownModule(owner string) bool { return owner == "true\t"+modulePath }

func TestSimilarModuleNames(t *testing.T) {
	for _, suffix := range []string{"_extra", ".other", "/nested"} {
		if ownModule("true\t" + modulePath + suffix) {
			t.Error(suffix)
		}
	}
}
