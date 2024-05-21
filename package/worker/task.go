package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type Task struct {
	ID              string             `json:"id"`
	Type            string             `json:"type"`
	Payload         string             `json:"payload"`
	Retries         int                `json:"retries"`
	Priority        uint8              `json:"priority"` // 0 to 9
	ScheduleAt      time.Time          `json:"schedule_at"`
	CreatedAt       time.Time          `json:"created_at"`
	Timeout         time.Duration      `json:"timeout"`
	Context         context.Context    `json:"-"`
	ContextCancel   context.CancelFunc `json:"-"`
	RetryDelay      time.Duration      `json:"retry_delay"`
	MiddlewareChain []func(task *Task) error
}

func NewTask(id, taskType, payload string, priority uint8, scheduleAt time.Time, timeout, retryDelay time.Duration) *Task {
	ctx, cancel := context.WithCancel(context.Background())
	return &Task{
		ID:            id,
		Type:          taskType,
		Payload:       payload,
		Priority:      priority,
		ScheduleAt:    scheduleAt,
		CreatedAt:     time.Now(),
		Timeout:       timeout,
		Context:       ctx,
		ContextCancel: cancel,
		RetryDelay:    retryDelay,
	}
}

func (t *Task) ToJSON() (string, error) {
	// Define a struct for marshaling without MiddlewareChain
	type taskWithoutMiddleware struct {
		ID         string        `json:"id"`
		Type       string        `json:"type"`
		Payload    string        `json:"payload"`
		Retries    int           `json:"retries"`
		Priority   uint8         `json:"priority"`
		ScheduleAt time.Time     `json:"schedule_at"`
		CreatedAt  time.Time     `json:"created_at"`
		Timeout    time.Duration `json:"timeout"`
		RetryDelay time.Duration `json:"retry_delay"`
	}

	// Create an instance of the struct without MiddlewareChain
	taskWithoutMiddlewareJSON := taskWithoutMiddleware{
		ID:         t.ID,
		Type:       t.Type,
		Payload:    t.Payload,
		Retries:    t.Retries,
		Priority:   t.Priority,
		ScheduleAt: t.ScheduleAt,
		CreatedAt:  t.CreatedAt,
		Timeout:    t.Timeout,
		RetryDelay: t.RetryDelay,
	}

	// Marshal the struct without MiddlewareChain
	bytes, err := json.Marshal(taskWithoutMiddlewareJSON)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (t *Task) ToByte() ([]byte, error) {
	// Define a struct for marshaling without MiddlewareChain
	type taskWithoutMiddleware struct {
		ID         string        `json:"id"`
		Type       string        `json:"type"`
		Payload    string        `json:"payload"`
		Retries    int           `json:"retries"`
		Priority   uint8         `json:"priority"`
		ScheduleAt time.Time     `json:"schedule_at"`
		CreatedAt  time.Time     `json:"created_at"`
		Timeout    time.Duration `json:"timeout"`
		RetryDelay time.Duration `json:"retry_delay"`
	}

	// Create an instance of the struct without MiddlewareChain
	taskWithoutMiddlewareJSON := taskWithoutMiddleware{
		ID:         t.ID,
		Type:       t.Type,
		Payload:    t.Payload,
		Retries:    t.Retries,
		Priority:   t.Priority,
		ScheduleAt: t.ScheduleAt,
		CreatedAt:  t.CreatedAt,
		Timeout:    t.Timeout,
		RetryDelay: t.RetryDelay,
	}

	// Marshal the struct without MiddlewareChain
	bytes, err := json.Marshal(taskWithoutMiddlewareJSON)

	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func TaskFromJSON(data string) (*Task, error) {
	var task Task
	err := json.Unmarshal([]byte(data), &task)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	task.Context = ctx
	task.ContextCancel = cancel
	return &task, nil
}

func (t *Task) IncrementRetries(maxRetries int) error {
	t.Retries++
	if t.Retries > maxRetries {
		return errors.New("max retries exceeded")
	}
	return nil
}

func (t *Task) IsExpired(ttl time.Duration) bool {
	return time.Since(t.CreatedAt) > ttl
}

func (t *Task) IsTimeout() bool {
	return t.Timeout != 0 && time.Since(t.CreatedAt) > t.Timeout
}

func (t *Task) AddMiddleware(middleware func(task *Task) error) {
	t.MiddlewareChain = append(t.MiddlewareChain, middleware)
}

func (t *Task) ExecuteMiddleware() error {
	for _, middleware := range t.MiddlewareChain {
		err := middleware(t)
		if err != nil {
			return err
		}
	}
	return nil
}
