//go:build !plan9
// +build !plan9

package firebirdsql

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
)

type Subscription struct {
	workerDone       func()
	mu               sync.RWMutex
	revent           *remoteEvent
	auxHandle        int32
	callback         EventHandler
	chEvent          chan Event
	eventCounts      chan Event
	closed           int32
	muClose          sync.Mutex
	closes           []chan error
	owner            *FbEvent
	noticeWorkers    sync.WaitGroup
	noticeCancel     chan struct{}
	noticeCanceled   bool
	terminalErr      error
	doneSubscription chan struct{}
	manager          *eventManager
	fc               *firebirdsqlConn
	scope            *eventScope
	onDone           func(*Subscription)
}

func newSubscription(scope *eventScope, dsn *firebirdDsn, events []string, cb EventHandler, ch chan Event) (*Subscription, error) {
	revent := newRemoteEvent()
	if err := revent.queueEvents(events...); err != nil {
		return nil, err
	}
	fc, err := newFirebirdsqlConnWithWire(dsn, scope.wire)
	if err != nil {
		return nil, err
	}
	s := &Subscription{fc: fc, scope: scope, revent: revent, callback: cb, chEvent: ch, eventCounts: make(chan Event), doneSubscription: make(chan struct{}), noticeCancel: make(chan struct{})}
	s.manager, err = s.getEventManager()
	if err != nil {
		return nil, err
	}
	if err = s.queueEvents(0); err != nil {
		return nil, fmt.Errorf("initial event registration: %w", err)
	}
	return s, nil
}
func (s *Subscription) start() {
	go func() { defer s.workerDone(); s.wait(s.manager.wait(s.revent, s.eventCounts)) }()
}
func (s *Subscription) wait(chErr <-chan error) {
	defer func() {
		s.stop(nil)
		s.scope.close()
		<-s.manager.done
		if s.onDone != nil {
			s.onDone(s)
		}
		close(s.doneSubscription)
	}()
	for {
		select {
		case <-s.scope.ctx.Done():
			return
		case err := <-chErr:
			s.stop(err)
			return
		case event := <-s.eventCounts:
			if s.callback != nil {
				go s.callback(event)
			} else {
				select {
				case s.chEvent <- event:
				case <-s.scope.ctx.Done():
					return
				}
			}
			if err := s.queueEvents(event.ID); err != nil {
				s.stop(fmt.Errorf("event rearm: %w", err))
				return
			}
		}
	}
}

// stop publishes the terminal state under the same lock used by NotifyClose.
// Transport cleanup never waits for a consumer to receive a notification.
func (s *Subscription) stop(err error) bool {
	s.muClose.Lock()
	defer s.muClose.Unlock()
	if s.IsClose() {
		return false
	}
	s.terminalErr = err
	atomic.StoreInt32(&s.closed, 1)
	if err != nil {
		for _, c := range s.closes {
			s.deliverNoticeLocked(c, err)
		}
	}
	s.closes = nil
	s.scope.cancel()
	return true
}

func (s *Subscription) terminalError() error {
	s.muClose.Lock()
	defer s.muClose.Unlock()
	return s.terminalErr
}

// deliverNoticeLocked registers every delivery before shutdown can start waiting.
// The owner also joins deliveries after the subscription leaves its active list.
func (s *Subscription) deliverNoticeLocked(receiver chan error, err error) {
	if s.noticeCanceled {
		return
	}
	s.owner.mu.Lock()
	if s.owner.IsClosed() {
		s.owner.mu.Unlock()
		return
	}
	s.owner.workers.Add(1)
	s.noticeWorkers.Add(1)
	s.owner.mu.Unlock()
	go func() {
		defer s.owner.workers.Done()
		defer s.noticeWorkers.Done()
		select {
		case receiver <- err:
		case <-s.noticeCancel:
		case <-s.owner.ctx.Done():
		}
	}()
}

// NotifyClose delivers an asynchronous terminal error, including to an unbuffered
// or temporarily busy receiver. Explicit Subscription/FbEvent Close may cancel
// pending delivery; normal shutdown sends no error. Receivers must not close
// registered channels. Late observers see the same terminal error until Close.
func (s *Subscription) NotifyClose(receiver chan error) {
	s.muClose.Lock()
	defer s.muClose.Unlock()
	if s.IsClose() {
		if s.terminalErr != nil {
			s.deliverNoticeLocked(receiver, s.terminalErr)
		}
		return
	}
	if !s.noticeCanceled {
		s.closes = append(s.closes, receiver)
	}
}
func (s *Subscription) IsClose() bool { return s == nil || atomic.LoadInt32(&s.closed) == 1 }

func (s *Subscription) cancelNotices() {
	s.muClose.Lock()
	if !s.noticeCanceled {
		s.noticeCanceled = true
		close(s.noticeCancel)
	}
	s.muClose.Unlock()
}
func (s *Subscription) finishClose() error {
	s.cancelNotices()
	<-s.doneSubscription
	s.noticeWorkers.Wait()
	return s.scope.close()
}

// Unsubscribe preserves the legacy no-op result for an already closed subscription.
func (s *Subscription) Unsubscribe() error {
	err := s.Close()
	if err == ErrFbEventClosed {
		return nil
	}
	return err
}
func (s *Subscription) Close() error {
	s.cancelNotices()
	first := s.stop(nil)
	err := s.finishClose()
	if !first {
		return ErrFbEventClosed
	}
	return err
}
func (s *Subscription) queueEvents(eventID int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := eventID + 1
	epbData := s.revent.buildEpb()

	if err := s.fc.wp.opQueEvents(s.auxHandle, epbData, id); err != nil {
		return err
	}
	rid, _, _, err := s.fc.wp.opResponse()
	if err != nil {
		return err
	}

	atomic.StoreInt32(&s.revent.id, id)
	atomic.StoreInt32(&s.revent.rid, rid)
	return nil
}

func (s *Subscription) getEventManager() (*eventManager, error) {
	auxHandle, addr, port, err := s.connAuxRequest()
	if err != nil {
		return nil, err
	}
	address := addr.String() + ":" + strconv.Itoa(port)
	newManager, err := newEventManager(s.scope, address, auxHandle)
	if err != nil {
		return nil, err
	}
	s.auxHandle = auxHandle
	return newManager, nil
}

func (s *Subscription) connAuxRequest() (int32, *net.IP, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fc.wp.opConnectRequest(); err != nil {
		return -1, nil, 0, err
	}
	auxHandle, _, buf, err := s.fc.wp.opResponse()
	if err != nil {
		return -1, nil, 0, err
	}
	if len(buf) < 8 {
		return -1, nil, 0, fmt.Errorf("truncated auxiliary address")
	}
	family := bytes_to_int16(buf[0:2])
	port := binary.BigEndian.Uint16(buf[2:4])
	ip := net.IPv4(buf[4], buf[5], buf[6], buf[7])

	if syscall.AF_INET != family {
		return -1, nil, 0, fmt.Errorf("unsupported  family protocol: %x", family)
	}

	return auxHandle, &ip, int(port), nil
}
