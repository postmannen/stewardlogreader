//go:build linux
// +build linux

package main

import "gopkg.in/fsnotify.v1"

var notifyOp = fsnotify.Create
