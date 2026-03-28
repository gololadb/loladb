package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/engine"
	loladbsql "github.com/jespino/loladb/pkg/sql"
)

func runTUI(path string) {
	eng, err := engine.Open(path, 256)
	if err != nil {
		fatal(fmt.Sprintf("Failed to open database: %v", err))
	}

	cat, err := catalog.New(eng)
	if err != nil {
		eng.Close()
		fatal(fmt.Sprintf("Failed to load catalog: %v", err))
	}

	ex := loladbsql.NewExecutor(cat)
	m := newTUIModel(path, eng, cat, ex)
	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		eng.Close()
		fatal(fmt.Sprintf("TUI error: %v", err))
	}
	eng.Close()
}

// --- Styles ---

var (
	titleBarStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("62")).
			Foreground(lipgloss.Color("230")).
			Bold(true).
			Padding(0, 1)

	statusBarStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("236")).
			Foreground(lipgloss.Color("248")).
			Padding(0, 1)

	statusKeyStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("62")).
			Foreground(lipgloss.Color("230")).
			Bold(true).
			Padding(0, 1)

	resultBorderStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("62"))

	inputBorderStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("205"))

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196")).
			Bold(true)

	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("78"))

	dimStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("241"))

	headerStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("212")).
			Bold(true)

	separatorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("240"))

	nullStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("241")).
			Italic(true)
)

// --- Model ---

type historyEntry struct {
	query    string
	result   string
	isError  bool
	duration time.Duration
	rows     int
}

type tuiModel struct {
	path      string
	eng       *engine.Engine
	cat       *catalog.Catalog
	exec      *loladbsql.Executor
	input     textarea.Model
	results   viewport.Model
	history   []historyEntry
	histIdx   int // for up/down history navigation (-1 = current input)
	savedInput string
	width     int
	height    int
	status    string
	ready     bool
}

func newTUIModel(path string, eng *engine.Engine, cat *catalog.Catalog, ex *loladbsql.Executor) tuiModel {
	ta := textarea.New()
	ta.Placeholder = "Type SQL here... (Enter to execute, Shift+Enter for newline)"
	ta.Focus()
	ta.CharLimit = 4096
	ta.SetWidth(80)
	ta.SetHeight(3)
	ta.ShowLineNumbers = false
	ta.FocusedStyle.CursorLine = lipgloss.NewStyle()
	ta.FocusedStyle.Base = lipgloss.NewStyle()

	vp := viewport.New(80, 20)

	tables, _ := cat.ListTables()
	tableCount := len(tables)

	return tuiModel{
		path:    path,
		eng:     eng,
		cat:     cat,
		exec:    ex,
		input:   ta,
		results: vp,
		histIdx: -1,
		status:  fmt.Sprintf("Connected to %s | %d tables", path, tableCount),
	}
}

func (m tuiModel) Init() tea.Cmd {
	return textarea.Blink
}

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

		inputHeight := 5
		resultHeight := m.height - inputHeight - 4 // title + status + borders
		if resultHeight < 3 {
			resultHeight = 3
		}

		m.input.SetWidth(m.width - 4)
		m.input.SetHeight(inputHeight - 2)
		m.results.Width = m.width - 4
		m.results.Height = resultHeight
		m.ready = true

		if len(m.history) == 0 {
			m.results.SetContent(m.renderWelcome())
		}
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		case "ctrl+d":
			if m.input.Value() == "" {
				return m, tea.Quit
			}
		case "enter":
			// Execute the query.
			query := strings.TrimSpace(m.input.Value())
			if query == "" {
				return m, nil
			}

			// Handle meta-commands.
			if strings.HasPrefix(query, "\\") {
				m.handleMeta(query)
				m.input.Reset()
				m.histIdx = -1
				return m, nil
			}

			// Remove trailing semicolons.
			query = strings.TrimRight(query, ";")
			if query == "" {
				return m, nil
			}

			m.executeQuery(query)
			m.input.Reset()
			m.histIdx = -1
			return m, nil

		case "up":
			// History navigation when input is on first line.
			if m.input.Line() == 0 {
				m.navigateHistory(-1)
				return m, nil
			}
		case "down":
			if m.input.Line() == m.input.LineCount()-1 && m.histIdx >= 0 {
				m.navigateHistory(1)
				return m, nil
			}
		case "ctrl+l":
			// Clear results.
			m.history = nil
			m.results.SetContent(m.renderWelcome())
			return m, nil
		case "pgup":
			m.results.ViewUp()
			return m, nil
		case "pgdown":
			m.results.ViewDown()
			return m, nil
		}
	}

	// Forward to textarea.
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m *tuiModel) executeQuery(query string) {
	start := time.Now()
	r, err := m.exec.Exec(query)
	elapsed := time.Since(start)

	entry := historyEntry{
		query:    query,
		duration: elapsed,
	}

	if err != nil {
		entry.result = errorStyle.Render("ERROR: " + err.Error())
		entry.isError = true
	} else {
		entry.result = m.renderResult(r)
		entry.rows = len(r.Rows)
		if r.RowsAffected > 0 {
			entry.rows = int(r.RowsAffected)
		}
	}

	m.history = append(m.history, entry)
	m.results.SetContent(m.renderAllHistory())
	m.results.GotoBottom()

	// Update status.
	tables, _ := m.cat.ListTables()
	if err != nil {
		m.status = fmt.Sprintf("Error | %s | %d tables", m.path, len(tables))
	} else {
		m.status = fmt.Sprintf("%.1fms | %d rows | %s | %d tables",
			float64(elapsed.Microseconds())/1000.0,
			entry.rows, m.path, len(tables))
	}
}

func (m *tuiModel) handleMeta(cmd string) {
	parts := strings.Fields(cmd)
	entry := historyEntry{query: cmd}

	switch parts[0] {
	case "\\q", "\\quit":
		// Handled by returning Quit, but let's also support it here.
		entry.result = "Bye!"
	case "\\dt":
		tables, _ := m.cat.ListTables()
		var sb strings.Builder
		sb.WriteString(headerStyle.Render("Tables") + "\n")
		if len(tables) == 0 {
			sb.WriteString(dimStyle.Render("  (no tables)") + "\n")
		}
		for _, t := range tables {
			stats, _ := m.cat.Stats(t.Name)
			tuples := int64(0)
			if stats != nil {
				tuples = stats.TupleCount
			}
			sb.WriteString(fmt.Sprintf("  %-20s  %d pages, %d rows\n", t.Name, t.Pages, tuples))
		}
		entry.result = sb.String()
	case "\\di":
		tables, _ := m.cat.ListTables()
		var sb strings.Builder
		sb.WriteString(headerStyle.Render("Indexes") + "\n")
		count := 0
		for _, t := range tables {
			idxs, _ := m.cat.ListIndexesForTable(t.OID)
			for _, idx := range idxs {
				cols, _ := m.cat.GetColumns(t.OID)
				colName := "?"
				if int(idx.ColNum-1) < len(cols) {
					colName = cols[idx.ColNum-1].Name
				}
				sb.WriteString(fmt.Sprintf("  %-20s  on %s(%s)\n", idx.Name, t.Name, colName))
				count++
			}
		}
		if count == 0 {
			sb.WriteString(dimStyle.Render("  (no indexes)") + "\n")
		}
		entry.result = sb.String()
	case "\\d":
		if len(parts) < 2 {
			entry.result = dimStyle.Render("Usage: \\d <table>")
		} else {
			tableName := parts[1]
			rel, err := m.cat.FindRelation(tableName)
			if err != nil || rel == nil {
				entry.result = errorStyle.Render(fmt.Sprintf("Table %q not found", tableName))
				entry.isError = true
			} else {
				cols, _ := m.cat.GetColumns(rel.OID)
				var sb strings.Builder
				sb.WriteString(headerStyle.Render(fmt.Sprintf("Table: %s", tableName)) + "\n")
				for _, c := range cols {
					sb.WriteString(fmt.Sprintf("  %-20s  %s\n", c.Name, typeName(c.Type)))
				}
				// Show indexes.
				idxs, _ := m.cat.ListIndexesForTable(rel.OID)
				if len(idxs) > 0 {
					sb.WriteString("\n" + headerStyle.Render("Indexes:") + "\n")
					for _, idx := range idxs {
						colName := "?"
						if int(idx.ColNum-1) < len(cols) {
							colName = cols[idx.ColNum-1].Name
						}
						sb.WriteString(fmt.Sprintf("  %s (%s)\n", idx.Name, colName))
					}
				}
				entry.result = sb.String()
			}
		}
	case "\\clear":
		m.history = nil
		m.results.SetContent(m.renderWelcome())
		return
	case "\\help", "\\?":
		var sb strings.Builder
		sb.WriteString(headerStyle.Render("Commands") + "\n")
		sb.WriteString("  \\dt            List tables\n")
		sb.WriteString("  \\di            List indexes\n")
		sb.WriteString("  \\d <table>     Describe table\n")
		sb.WriteString("  \\clear         Clear output\n")
		sb.WriteString("  \\help          Show this help\n")
		sb.WriteString("  \\q             Quit\n")
		sb.WriteString("\n" + headerStyle.Render("Keys") + "\n")
		sb.WriteString("  Enter          Execute query\n")
		sb.WriteString("  Shift+Enter    Newline in query\n")
		sb.WriteString("  Up/Down        History navigation\n")
		sb.WriteString("  PgUp/PgDn      Scroll results\n")
		sb.WriteString("  Ctrl+L         Clear results\n")
		sb.WriteString("  Ctrl+C         Quit\n")
		entry.result = sb.String()
	default:
		entry.result = errorStyle.Render(fmt.Sprintf("Unknown command: %s (try \\help)", parts[0]))
		entry.isError = true
	}

	m.history = append(m.history, entry)
	m.results.SetContent(m.renderAllHistory())
	m.results.GotoBottom()
}

func (m *tuiModel) navigateHistory(dir int) {
	if len(m.history) == 0 {
		return
	}

	if m.histIdx == -1 {
		m.savedInput = m.input.Value()
	}

	m.histIdx += dir

	if m.histIdx < 0 {
		m.histIdx = -1
		m.input.SetValue(m.savedInput)
		return
	}
	if m.histIdx >= len(m.history) {
		m.histIdx = len(m.history) - 1
	}

	// Navigate from newest to oldest.
	idx := len(m.history) - 1 - m.histIdx
	if idx >= 0 && idx < len(m.history) {
		m.input.SetValue(m.history[idx].query)
	}
}

func (m *tuiModel) renderResult(r *loladbsql.Result) string {
	if r.Message != "" && len(r.Rows) == 0 {
		return successStyle.Render(r.Message)
	}

	if len(r.Columns) == 0 || len(r.Rows) == 0 {
		if r.Message != "" {
			return successStyle.Render(r.Message)
		}
		return dimStyle.Render("(0 rows)")
	}

	// Calculate column widths.
	widths := make([]int, len(r.Columns))
	for i, c := range r.Columns {
		widths[i] = len(c)
	}
	for _, row := range r.Rows {
		for i, d := range row {
			if i < len(widths) {
				s := datumString(d)
				if len(s) > widths[i] {
					widths[i] = len(s)
				}
				if widths[i] > 30 {
					widths[i] = 30
				}
			}
		}
	}
	for i := range widths {
		if widths[i] < 4 {
			widths[i] = 4
		}
	}

	var sb strings.Builder

	// Header.
	var hdr []string
	for i, c := range r.Columns {
		hdr = append(hdr, headerStyle.Render(padRight(c, widths[i])))
	}
	sb.WriteString(strings.Join(hdr, " "+separatorStyle.Render("|")+" "))
	sb.WriteString("\n")

	// Separator.
	var sep []string
	for _, w := range widths {
		sep = append(sep, separatorStyle.Render(strings.Repeat("─", w)))
	}
	sb.WriteString(strings.Join(sep, separatorStyle.Render("─┼─")))
	sb.WriteString("\n")

	// Data rows.
	for _, row := range r.Rows {
		var vals []string
		for i, d := range row {
			if i >= len(widths) {
				break
			}
			s := datumString(d)
			if d.Type == 0 { // NULL
				s = nullStyle.Render(padRight("NULL", widths[i]))
			} else {
				s = padRight(s, widths[i])
			}
			vals = append(vals, s)
		}
		sb.WriteString(strings.Join(vals, " "+separatorStyle.Render("|")+" "))
		sb.WriteString("\n")
	}

	sb.WriteString(dimStyle.Render(fmt.Sprintf("(%d rows)", len(r.Rows))))
	return sb.String()
}

func (m *tuiModel) renderAllHistory() string {
	var sb strings.Builder
	for i, entry := range m.history {
		if i > 0 {
			sb.WriteString("\n")
		}
		// Show the query.
		sb.WriteString(dimStyle.Render("loladb> "))
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("117")).Render(entry.query))
		sb.WriteString("\n")

		// Show the result.
		sb.WriteString(entry.result)
		sb.WriteString("\n")

		if !entry.isError && entry.duration > 0 {
			sb.WriteString(dimStyle.Render(fmt.Sprintf("Time: %.3fms", float64(entry.duration.Microseconds())/1000.0)))
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

func (m *tuiModel) renderWelcome() string {
	var sb strings.Builder

	logo := lipgloss.NewStyle().
		Foreground(lipgloss.Color("205")).
		Bold(true).
		Render(`
  ██╗      ██████╗ ██╗      █████╗ ██████╗ ██████╗
  ██║     ██╔═══██╗██║     ██╔══██╗██╔══██╗██╔══██╗
  ██║     ██║   ██║██║     ███████║██║  ██║██████╔╝
  ██║     ██║   ██║██║     ██╔══██║██║  ██║██╔══██╗
  ███████╗╚██████╔╝███████╗██║  ██║██████╔╝██████╔╝
  ╚══════╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═════╝ ╚═════╝`)

	sb.WriteString(logo)
	sb.WriteString("\n\n")
	sb.WriteString(dimStyle.Render("  Type SQL queries and press Enter to execute."))
	sb.WriteString("\n")
	sb.WriteString(dimStyle.Render("  Type \\help for commands, \\dt for tables, Ctrl+C to quit."))
	sb.WriteString("\n\n")

	tables, _ := m.cat.ListTables()
	if len(tables) > 0 {
		sb.WriteString(headerStyle.Render("  Tables:") + "\n")
		for _, t := range tables {
			stats, _ := m.cat.Stats(t.Name)
			tuples := int64(0)
			if stats != nil {
				tuples = stats.TupleCount
			}
			sb.WriteString(fmt.Sprintf("    %-20s  %d rows\n", t.Name, tuples))
		}
	} else {
		sb.WriteString(dimStyle.Render("  No tables yet. Try: CREATE TABLE users (id INT, name TEXT)"))
		sb.WriteString("\n")
	}

	return sb.String()
}

func (m tuiModel) View() string {
	if !m.ready {
		return "Loading..."
	}

	// Title bar.
	title := titleBarStyle.Width(m.width).Render(" LolaDB SQL Shell ")

	// Results area.
	resultBox := resultBorderStyle.Width(m.width - 2).Render(m.results.View())

	// Input area.
	inputBox := inputBorderStyle.Width(m.width - 2).Render(m.input.View())

	// Status bar.
	leftStatus := statusKeyStyle.Render(" SQL ")
	rightStatus := statusBarStyle.Width(m.width - lipgloss.Width(leftStatus) - 1).Render(m.status)
	statusBar := lipgloss.JoinHorizontal(lipgloss.Top, leftStatus, rightStatus)

	return lipgloss.JoinVertical(lipgloss.Left,
		title,
		resultBox,
		inputBox,
		statusBar,
	)
}
