package menghuibasic

import "time"

func StopTimer(timers ...*time.Timer) {
	for _, timer := range timers {
		if timer != nil {
			timer.Stop()
		}
	}
}
func DeleteTimer(timers ...*time.Timer) {
	for _, timer := range timers {
		StopTimer(timer)
		timer = nil
	}
}
