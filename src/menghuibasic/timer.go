package menghuibasic

import "time"

func StopTimer(timer *time.Timer) {
	if timer != nil {
		// If by the time we stop the timer, the timer is already stopped/triggered
		if !timer.Stop() && len(timer.C) > 0 {
			<-timer.C // Drain the channel
		}
	}
}

func StopTimers(timers ...*time.Timer) {
	for _, timer := range timers {
		StopTimer(timer)
	}
}
