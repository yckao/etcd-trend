package tui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/term"
)

type response struct {
	generation uint64
	data       Data
}

// Snapshot writes a deterministic plain-text view; it does not alter terminal
// state and is useful over pipes, in logs, and for reproduction of a selection.
func Snapshot(ctx context.Context, store Store, cfg Config, out io.Writer) error {
	s, err := NewState(store.Info(), cfg)
	if err != nil {
		return err
	}
	s.Data = s.Query(ctx, store)
	if s.Data.Err != nil {
		return s.Data.Err
	}
	_, err = io.WriteString(out, s.Render())
	return err
}

// Run enters an alternate screen, keeps input responsive while disk queries run,
// and restores the original terminal on quit, signal, query errors or I/O errors.
func Run(ctx context.Context, store Store, cfg Config, in, out *os.File) (err error) {
	fd := int(in.Fd())
	outputFD := int(out.Fd())
	if !term.IsTerminal(fd) || !term.IsTerminal(outputFD) {
		return fmt.Errorf("interactive TUI requires a terminal; use --snapshot for plain output")
	}
	width, height, sizeErr := term.GetSize(outputFD)
	if sizeErr == nil {
		cfg.Width, cfg.Height = width, height
	}
	s, err := NewState(store.Info(), cfg)
	if err != nil {
		return err
	}
	old, err := term.MakeRaw(fd)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, term.Restore(fd, old)) }()
	if _, err = io.WriteString(out, "\x1b[?1049h\x1b[?25l"); err != nil {
		return err
	}
	defer func() { _, e := io.WriteString(out, "\x1b[0m\x1b[?25h\x1b[?1049l"); err = errors.Join(err, e) }()
	runCtx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	keys := make(chan string, 64)
	inputErr := make(chan error, 1)
	workers.Add(1)
	go func() { defer workers.Done(); inputErr <- readKeys(runCtx, fd, keys) }()
	responses := make(chan response, 8)
	var generation uint64
	var stopQuery context.CancelFunc
	defer func() {
		if stopQuery != nil {
			stopQuery()
		}
	}()
	query := func() {
		if stopQuery != nil {
			stopQuery()
		}
		generation++
		id := generation
		queryCtx, stop := context.WithCancel(runCtx)
		stopQuery = stop
		s.Loading = true
		snapshot := *s
		// Keep the global overview visible while discarding stale interval data.
		s.Data = Data{Curve: s.Data.Curve}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer stop()
			d := snapshot.Query(queryCtx, store)
			select {
			case responses <- response{id, d}:
			case <-runCtx.Done():
			}
		}()
	}
	paint := func() error {
		lines := strings.Split(strings.TrimSuffix(s.Render(), "\n"), "\n")
		for i, line := range lines {
			if i == 0 {
				lines[i] = "\x1b[1;36m" + line + "\x1b[0m"
			} else if strings.HasPrefix(line, "> ") {
				lines[i] = "\x1b[7m" + line + "\x1b[0m"
			}
		}
		_, e := io.WriteString(out, "\x1b[H\x1b[2J"+strings.Join(lines, "\r\n"))
		return e
	}
	query()
	if err = paint(); err != nil {
		return err
	}
	resize := time.NewTicker(200 * time.Millisecond)
	defer resize.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-inputErr:
			if errors.Is(e, context.Canceled) || errors.Is(e, io.EOF) {
				return nil
			}
			return e
		case key := <-keys:
			refresh, quit := s.Key(key)
			if quit {
				return nil
			}
			if refresh {
				query()
			}
			if err = paint(); err != nil {
				return err
			}
		case result := <-responses:
			if result.generation != generation {
				continue
			}
			s.Loading = false
			s.Data = result.data
			if s.Selected >= s.rowCount() {
				s.Selected = max(0, s.rowCount()-1)
			}
			if err = paint(); err != nil {
				return err
			}
		case <-resize.C:
			w, h, e := term.GetSize(outputFD)
			if e == nil && s.Resize(w, h) {
				query()
				if err = paint(); err != nil {
					return err
				}
			}
		}
	}
}
