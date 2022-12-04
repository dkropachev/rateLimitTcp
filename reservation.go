package rateLimitTcp

import (
	"time"

	"golang.org/x/time/rate"
)

type WrappedReservation struct {
	global *rate.Reservation
	local  *rate.Reservation
}

func (r *WrappedReservation) OK() bool {
	if r.local == nil {
		return r.global.OK()
	}
	return r.global.OK() && r.local.OK()
}

func (r *WrappedReservation) Delay() time.Duration {
	return r.DelayFrom(time.Now())
}

func (r *WrappedReservation) DelayFrom(t time.Time) time.Duration {
	if r.local == nil {
		return r.global.DelayFrom(t)
	}
	gd := r.global.DelayFrom(t)
	ld := r.local.DelayFrom(t)
	if gd > ld {
		return gd
	}
	return ld
}

func (r *WrappedReservation) Cancel() {
	r.CancelAt(time.Now())
}

func (r *WrappedReservation) CancelAt(t time.Time) {
	r.global.CancelAt(t)
	if r.local != nil {
		r.local.CancelAt(t)
	}
}
