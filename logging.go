package main

import (
	"log"
	"net/http"
	"time"
)

// responseWriter is a minimal wrapper for http.ResponseWriter that allows the
// written HTTP status code to be captured for logging.
type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func wrapResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w}
}

func (rw *responseWriter) Status() int {
	return rw.status
}

func (rw *responseWriter) WriteHeader(code int) {
	if rw.wroteHeader {
		return
	}

	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
	rw.wroteHeader = true

	return
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				log.Printf("Middleware error: %v", err)
			}
		}()

		start := time.Now()
		// Call the next handler
		wrapped := wrapResponseWriter(w)
		next.ServeHTTP(wrapped, r)

		log.Printf(
			"%v\t%s\t%s\t\t%s\t%s",
			wrapped.status,
			r.Method,
			r.URL.EscapedPath(),
			r.RemoteAddr,
			time.Since(start),
		)
	})
}
