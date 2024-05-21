package utils

import (
	"bytes"
	"os/exec"
)

const commandType = "bash"

func Command(_command string) (string, string, error) {
	var stdout bytes.Buffer

	var stderr bytes.Buffer

	cmd := exec.Command(commandType, "-c", _command)

	cmd.Stdout = &stdout

	cmd.Stderr = &stderr

	err := cmd.Run()

	return stdout.String(), stderr.String(), err
}
