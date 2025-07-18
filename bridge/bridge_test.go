package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDataFrame(t *testing.T) {
	handle := NewDataFrame()
	assert.NotEqual(t, handle, -1, "NewDataFrame should return a valid handle")
}
