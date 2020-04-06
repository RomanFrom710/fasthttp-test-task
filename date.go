package main

import "time"

func getDayBeginning(tm time.Time) time.Time {
	year, month, day := tm.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func isDifferentDay(currentDay, tm time.Time) bool {
	diff := tm.Sub(currentDay)
	return diff.Hours() >= 24
}

func getTimeFromUnix(timestampMs int64) time.Time {
	seconds := timestampMs / 1000
	nanoseconds := (timestampMs % 1000) * 1000000
	return time.Unix(seconds, nanoseconds).UTC()
}
