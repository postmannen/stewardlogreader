//go:build darwin
// +build darwin

package main

import "gopkg.in/fsnotify.v1"

var notifyOp = fsnotify.Chmod
