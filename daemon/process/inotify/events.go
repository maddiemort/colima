package inotify

import (
	"context"
	"fmt"
	"io/fs"
	"time"

	"github.com/rjeczalik/notify"
)

type modEvent struct {
	path string // filename
	notify.Event
	fs.FileMode
}

func (m modEvent) Mode() string { return fmt.Sprintf("%o", m.FileMode) }

func (f *inotifyProcess) handleEvents(ctx context.Context, watcher dirWatcher) error {
	log := f.log
	log.Trace("begin inotify event handler")

	mod := make(chan modEvent)
	vols := make(chan []string)

	if err := f.monitorContainerVolumes(ctx, vols); err != nil {
		return fmt.Errorf("error watching container volumes: %w", err)
	}

	var last time.Time
	var cancelWatch context.CancelFunc
	var currentVols []string

	volsChanged := func(vols []string) bool {
		if len(currentVols) != len(vols) {
			return true
		}
		for i := range vols {
			if vols[i] != currentVols[i] {
				return true
			}
		}
		return false
	}

	cache := map[string]notify.Event{}

	for {
		select {

		// exit signal
		case <-ctx.Done():
			close(mod)
			return ctx.Err()

		// watch only container volumes
		case vols := <-vols:
			if !volsChanged(vols) {
				continue
			}
			log.Tracef("volumes changed from: %+v, to: %+v", currentVols, vols)

			currentVols = vols

			if cancel := cancelWatch; cancel != nil {
				// delay a bit to avoid zero downtime
				time.AfterFunc(time.Second*1, cancel)
			}

			ctx, cancel := context.WithCancel(ctx)
			cancelWatch = cancel

			go func(ctx context.Context, vols []string, mod chan<- modEvent) {
				if err := watcher.Watch(ctx, vols, mod); err != nil {
					log.Error(fmt.Errorf("error running watcher: %w", err))
				}
			}(ctx, vols, mod)

		// handle modification events
		case ev := <-mod:
			now := time.Now()
			log.Tracef("handling inotify event for %s", ev.path)

			// rate limit, handle at most 50 unique items every 500 ms
			if now.Sub(last) < time.Millisecond*500 {
				if event, ok := cache[ev.path]; ok && event != ev.Event {
					log.Tracef("inotify %s event for %s already handled in last 500 ms", event.String(), ev.path)
					continue // handled, ignore
				}
				if len(cache) > 50 {
					log.Tracef("cache contains %d items, skipping notify event for %s", len(cache), ev.path)
					continue
				}
			} else {
				log.Trace("resetting inotify event cache")
				last = now
				cache = map[string]notify.Event{} // >500ms, reset unique cache
			}

			// cache current event
			log.Tracef("caching inotify event for %s", ev.path)
			cache[ev.path] = ev.Event

			// validate that file exists
			if err := f.guest.RunQuiet("stat", ev.path); err != nil {
				log.Trace(fmt.Errorf("cannot stat '%s': %w", ev.path, err))
				continue
			}

			log.Infof("syncing inotify %s event for %s ", ev.Event.String(), ev.path)
			switch ev.Event {
			case notify.Write:
				if err := f.guest.RunQuiet("touch", "-m", ev.path); err != nil {
					log.Trace(fmt.Errorf("error syncing inotify event: %w", err))
				}
			default:
				if err := f.guest.RunQuiet("sudo", "/bin/chmod", ev.Mode(), ev.path); err != nil {
					log.Trace(fmt.Errorf("error syncing inotify event: %w", err))
				}
			}
		}
	}
}
