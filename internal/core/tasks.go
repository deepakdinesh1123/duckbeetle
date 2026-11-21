package core

import (
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/knadh/goyesql/v2"
)

// Task represents an SQL query task.
type Task struct {
	Name  string   `json:"name"`
	Queue string   `json:"queue"`
	Conc  int      `json:"concurrency"`
	Raw   string   `json:"raw,omitempty"`
	DBs   []string `json:"dbs,omitempty"`
}

// Tasks maps names to SQL query tasks.
type Tasks map[string]Task

// HasDB checks if the given db name is attached to this task.
func (t Task) HasDB(name string) bool {
	for _, db := range t.DBs {
		if db == name {
			return true
		}
	}
	return false
}

// LoadTasks loads SQL queries from all the .sql files in a given directory.
func (co *Core) LoadTasks(dirs []string) error {
	for _, d := range dirs {
		co.lo.Info("loading SQL queries from directory", "dir", d)
		tasks, err := co.loadTasks(d)
		if err != nil {
			return err
		}

		for t, q := range tasks {
			if _, ok := co.tasks[t]; ok {
				return fmt.Errorf("duplicate task %s", t)
			}

			co.tasks[t] = q
		}

		co.lo.Info("loaded tasks (SQL queries)", "count", len(tasks), "dir", d)
	}

	return nil
}

func (co *Core) loadTasks(dir string) (Tasks, error) {
	files, err := filepath.Glob(path.Join(dir, "*.sql"))
	if err != nil {
		return nil, fmt.Errorf("unable to read SQL directory %s: %v", dir, err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no SQL files found in %s", dir)
	}

	tasks := make(Tasks)
	for _, f := range files {
		q := goyesql.MustParseFile(f)

		for name, s := range q {
			if _, ok := tasks[string(name)]; ok {
				return nil, fmt.Errorf("duplicate query %s (%s)", name, f)
			}

			availableSources := co.engine.GetSources()
			var taskDBs []string
			if dbTag, ok := s.Tags["db"]; ok && strings.TrimSpace(dbTag) != "" {
				for _, db := range strings.Split(dbTag, ",") {
					if c := strings.TrimSpace(db); c != "" {
						if _, ok := availableSources[c]; !ok {
							return nil, fmt.Errorf("unrecognized source DB '%s' in %s (task: %s)", c, f, name)
						}
						taskDBs = append(taskDBs, c)
					}
				}
			} else {
				// If no specific DBs are attached to the query, attach all available DBs (DungBeetle behavior)
				for sName := range availableSources {
					taskDBs = append(taskDBs, sName)
				}
			}

			queue := co.opt.DefaultQueue
			if v, ok := s.Tags["queue"]; ok {
				queue = strings.TrimSpace(v)
			}

			conc := co.opt.DefaultGroupConcurrency
			if v, ok := s.Tags["conc"]; ok {
				var err error
				conc, err = strconv.Atoi(strings.TrimSpace(v))
				if err != nil {
					return nil, err
				}
			}

			co.lo.Info("loading task", "task", name, "db", taskDBs, "queue", queue)
			tasks[name] = Task{
				Name:  name,
				Queue: queue,
				Conc:  conc,
				Raw:   s.Query,
				DBs:   taskDBs,
			}
		}
	}

	return tasks, nil
}
