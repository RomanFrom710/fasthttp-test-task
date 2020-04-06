package main

import (
	"testing"
	"time"
)

func TestGetDayBeginning(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2020-04-06T16:36:46Z")
	expected, _ := time.Parse(time.RFC3339, "2020-04-06T00:00:00Z")

	dayBeginning := getDayBeginning(now)
	if dayBeginning != expected {
		t.Errorf("Got: %v, expected: %v.", dayBeginning, expected)
	}
}

func TestIsDifferentDay_DifferentDay(t *testing.T) {
	current, _ := time.Parse(time.RFC3339, "2020-04-03T00:00:00Z")
	other, _ := time.Parse(time.RFC3339, "2020-04-04T01:00:00Z")

	isDifferent := isDifferentDay(current, other)
	if !isDifferent {
		t.Error("Got false for different days.")
	}
}

func TestIsDifferentDay_SameDay(t *testing.T) {
	current, _ := time.Parse(time.RFC3339, "2020-04-03T00:00:00Z")
	other, _ := time.Parse(time.RFC3339, "2020-04-03T01:00:00Z")

	isDifferent := isDifferentDay(current, other)
	if isDifferent {
		t.Error("Got true for the same days.")
	}
}

func TestGetTimeFromUnix(t *testing.T) {
	msTimestamp := int64(1586191699722)
	expected, _ := time.Parse(time.RFC3339, "2020-04-06T16:48:19.722Z")

	tm := getTimeFromUnix(msTimestamp)
	if tm != expected {
		t.Errorf("Got: %v, expected: %v.", tm, expected)
	}
}
