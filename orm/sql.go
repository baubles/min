package orm

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type StatementType int

const (
	_ StatementType = iota
	_INSERT_STATEMENT
	_DELETE_STATEMENT
	_UPDATE_STATEMENT
	_SELECT_STATEMENT
)

type Sql interface {
	Update(table string) Sql
	Set(set string) Sql
	DeleteFrom(table string) Sql
	InsertInto(table string) Sql
	Columns(columns string) Sql
	Values(values string) Sql
	Select(columns string) Sql
	SelectDistinct(columns string) Sql
	From(tables string) Sql
	Join(join string) Sql
	InnerJoin(innerJoin string) Sql
	LeftJoin(leftOuterJoin string) Sql
	RightJoin(rightOuterJoin string) Sql
	OuterJoin(outJoin string) Sql
	Where(where string) Sql
	Having(having string) Sql
	And() Sql
	Or() Sql
	GroupBy(having string) Sql
	OrderBy(orderBy string) Sql
	Limit(limit string) Sql
	Sql(sqlString string) Sql
	SqlString() string
	Prepare() error
	Exec(args interface{}) (int64, error)
	QueryRow(args interface{}, row interface{}) error
	QueryRows(args interface{}, rows interface{}) (int64, error)
	Close() error
}

const (
	_ = iota
	_WHERE_CONDITION
	_HAVING_CONDITION
)

type rawSql struct {
	_select         []string
	_tables         []string
	_join           []string
	_innerJoin      []string
	_outerJoin      []string
	_leftOuterJoin  []string
	_rightOuterJoin []string
	_where          []string
	_having         []string
	_groupBy        []string
	_orderBy        []string
	_columns        []string
	_values         []string
	_sets           []string
	_limit          []string
	_sql            string
	_args           []interface{}
	_distinct       bool
	_lastCondition  int
	_statementType  StatementType

	sqlType StatementType
	tokens  []string
	rawSql  string

	conn DB
	stmt *sql.Stmt
}

const (
	AND = ") \n AND ("
	OR  = ") \n OR ("
)

func newSql() Sql {
	return new(rawSql)
}

func (s *rawSql) Update(table string) Sql {
	s._tables = append(s._tables, table)
	s._statementType = _UPDATE_STATEMENT
	return s
}

func (s *rawSql) Set(set string) Sql {
	s._sets = append(s._sets, set)
	return s
}

func (s *rawSql) DeleteFrom(table string) Sql {
	s._tables = append(s._tables, table)
	s._statementType = _DELETE_STATEMENT
	return s
}

func (s *rawSql) InsertInto(table string) Sql {
	s._tables = append(s._tables, table)
	s._statementType = _INSERT_STATEMENT
	return s
}

func (s *rawSql) Values(values string) Sql {
	s._values = append(s._values, values)
	return s
}

func (s *rawSql) Columns(columns string) Sql {
	s._columns = append(s._columns, columns)
	return s
}

func (s *rawSql) Select(columns string) Sql {
	s._select = append(s._select, columns)
	s._statementType = _SELECT_STATEMENT
	return s
}

func (s *rawSql) SelectDistinct(columns string) Sql {
	s._distinct = true
	return s.Select(columns)
}

func (s *rawSql) From(tables string) Sql {
	s._tables = append(s._tables, tables)
	return s
}

func (s *rawSql) Join(join string) Sql {
	s._join = append(s._join, join)
	return s
}

func (s *rawSql) InnerJoin(innerJoin string) Sql {
	s._innerJoin = append(s._innerJoin, innerJoin)
	return s
}

func (s *rawSql) LeftJoin(leftOuterJoin string) Sql {
	s._leftOuterJoin = append(s._leftOuterJoin, leftOuterJoin)
	return s
}

func (s *rawSql) RightJoin(rightOuterJoin string) Sql {
	s._rightOuterJoin = append(s._rightOuterJoin, rightOuterJoin)
	return s
}

func (s *rawSql) OuterJoin(outJoin string) Sql {
	s._outerJoin = append(s._outerJoin, outJoin)
	return s
}

func (s *rawSql) Where(where string) Sql {
	s._where = append(s._where, where)
	s._lastCondition = _WHERE_CONDITION
	return s
}

func (s *rawSql) Having(having string) Sql {
	s._having = append(s._having, having)
	s._lastCondition = _HAVING_CONDITION
	return s
}

func (s *rawSql) And() Sql {
	s.addCondition(AND)
	return s
}

func (s *rawSql) Or() Sql {
	s.addCondition(OR)
	return s
}

func (s *rawSql) addCondition(condition string) {
	switch s._lastCondition {
	case _WHERE_CONDITION:
		s._where = append(s._where, condition)
		break
	case _HAVING_CONDITION:
		s._having = append(s._having, condition)
		break
	}
}

func (s *rawSql) GroupBy(having string) Sql {
	s._groupBy = append(s._having, having)
	return s
}

func (s *rawSql) OrderBy(orderBy string) Sql {
	s._orderBy = append(s._orderBy, orderBy)
	return s
}

func (s *rawSql) Limit(limit string) Sql {
	s._limit = append(s._limit, limit)
	return s
}

func (s *rawSql) Sql(sqlString string) Sql {
	s._sql = sqlString
	return s
}

func sqlClause(buffer *bytes.Buffer, keyword string, parts []string, openWord string, closeWord string, conjunction string) {
	if len(parts) != 0 {
		if buffer.Len() != 0 {
			buffer.WriteString("\n")
		}
		buffer.WriteString(keyword)
		buffer.WriteString(" ")
		buffer.WriteString(openWord)
		last := "_________"
		for i, v := range parts {
			if i > 0 && v != AND && v != OR && last != AND && last != OR {
				buffer.WriteString(conjunction)
			}
			buffer.WriteString(v)
			last = v
		}
		buffer.WriteString(closeWord)
	}
}

func (s *rawSql) selectSQL(buffer *bytes.Buffer) {
	if s._distinct {
		sqlClause(buffer, "SELECT DISTINCT", s._select, "", "", ", ")
	} else {
		sqlClause(buffer, "SELECT", s._select, "", "", ", ")
	}

	sqlClause(buffer, "FROM", s._tables, "", "", ", ")
	sqlClause(buffer, "JOIN", s._join, "", "", "\nJOIN ")
	sqlClause(buffer, "INNER JOIN", s._innerJoin, "", "", "\nINNER JOIN ")
	sqlClause(buffer, "OUTER JOIN", s._outerJoin, "", "", "\nOUTER JOIN ")
	sqlClause(buffer, "LEFT OUTER JOIN", s._leftOuterJoin, "", "", "\nLEFT OUTER JOIN")
	sqlClause(buffer, "RIGHT OUTER JOIN", s._rightOuterJoin, "", "", "\nRIGHT OUTER JOIN")
	sqlClause(buffer, "WHERE", s._where, "(", ")", " AND ")
	sqlClause(buffer, "GROUP BY", s._groupBy, "", "", ", ")
	sqlClause(buffer, "HAVING", s._having, "(", ")", " AND ")
	sqlClause(buffer, "ORDER BY", s._orderBy, "", "", ", ")
	sqlClause(buffer, "LIMIT", s._limit, "", "", "")
}

func (s *rawSql) updateSQL(buffer *bytes.Buffer) {
	sqlClause(buffer, "UPDATE", s._tables, "", "", "")
	sqlClause(buffer, "SET", s._sets, "", "", ", ")
	sqlClause(buffer, "WHERE", s._where, "(", ")", " AND ")
}

func (s *rawSql) deleteSQL(buffer *bytes.Buffer) {
	sqlClause(buffer, "DELETE FROM", s._tables, "", "", "")
	sqlClause(buffer, "WHERE", s._where, "(", ")", " AND ")
}

func (s *rawSql) insertSQL(buffer *bytes.Buffer) {
	sqlClause(buffer, "INSERT INTO", s._tables, "", "", "")
	sqlClause(buffer, "", s._columns, "(", ")", ", ")
	sqlClause(buffer, "VALUES", s._values, "(", ")", ", ")
}

func (s *rawSql) SqlString() string {
	if s._sql == "" {
		buffer := new(bytes.Buffer)
		switch s._statementType {
		case _INSERT_STATEMENT:
			s.insertSQL(buffer)
			break
		case _DELETE_STATEMENT:
			s.deleteSQL(buffer)
			break
		case _UPDATE_STATEMENT:
			s.updateSQL(buffer)
			break
		case _SELECT_STATEMENT:
			s.selectSQL(buffer)
			break
		default:
			return ""
		}
		return buffer.String()
	}
	return s._sql
}

func (s *rawSql) build() {
	str := s.SqlString()
	s.sqlType = s._statementType
	s.rawSql, s.tokens = s.parseToken(str, "${", "}")
}

func getValue(root interface{}, field string) (interface{}, error) {
	return ReflectGetValue(root, field)
}

func (s *rawSql) parseToken(str, openToken, closeToken string) (string, []string) {
	buffer := new(bytes.Buffer)
	var tokens []string
	start := strings.Index(str, openToken)
	for start > -1 {
		if start > 0 && str[start-1] == '\\' {
			buffer.WriteString(str[:start])
			buffer.WriteString(openToken)
			str = str[start+len(openToken):]
		} else {
			offset := strings.Index(str[start:], closeToken)
			if offset == -1 {
				buffer.WriteString(str[:])
				str = ""
			} else {
				buffer.WriteString(str[:start])
				token := str[start+len(openToken) : start+offset]
				tokens = append(tokens, token)
				buffer.WriteString(s.handleToken(token))
				str = str[start+offset+len(closeToken):]
			}
			start = strings.Index(str, openToken)
		}
	}
	buffer.WriteString(str)
	return buffer.String(), tokens
}

func (s *rawSql) handleToken(token string) string {
	return "?"
}

func (s *rawSql) parseArgs(args interface{}) []interface{} {
	var buildArgs []interface{}
	switch reflect.ValueOf(args).Kind() {
	case reflect.Array, reflect.Slice:
		return args.([]interface{})
	default:
		for _, token := range s.tokens {
			v, ok := getValue(args, token)
			if ok == nil {
				buildArgs = append(buildArgs, v)
			}
		}
	}
	return buildArgs
}

func (s *rawSql) Prepare() error {
	s.build()
	var err error
	s.stmt, err = s.conn.Prepare(s.rawSql)
	return err
}

func (s *rawSql) exec(args interface{}) (sql.Result, error) {
	if s.stmt != nil {
		buildArgs := s.parseArgs(args)
		return s.stmt.Exec(buildArgs...)
	} else {
		s.build()
		buildArgs := s.parseArgs(args)
		return s.conn.Exec(s.rawSql, buildArgs...)
	}
}

func (s *rawSql) Exec(args interface{}) (int64, error) {
	result, err := s.exec(args)
	if err == nil {
		if s.sqlType == _INSERT_STATEMENT {
			return result.LastInsertId()
		} else {
			return result.RowsAffected()
		}
	}
	return int64(0), err
}

func (s *rawSql) query(args interface{}) (*sql.Rows, error) {
	if s.stmt != nil {
		buildArgs := s.parseArgs(args)
		return s.stmt.Query(buildArgs...)
	} else {
		s.build()
		buildArgs := s.parseArgs(args)
		if Debug {
			fmt.Println("[database.sql] sql:", s.rawSql, " args: ", buildArgs)
		}
		return s.conn.Query(s.rawSql, buildArgs...)
	}
}

func (s *rawSql) QueryRow(args interface{}, row interface{}) error {
	sqlRows, err := s.query(args)
	if err != nil {
		return err
	}
	typ := reflect.TypeOf(row)

	if typ.Kind() != reflect.Ptr || (typ.Elem().Kind() != reflect.Struct && typ.Elem().Kind() != reflect.Map) {
		return errors.New("[database.sql] rows must be ptr of map or struct")
	}

	values := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(row)), 0, 0)
	values = reflect.Append(values, reflect.ValueOf(row))
	ptr := reflect.New(values.Type())
	ptr.Elem().Set(values)
	n, err := s.parseRows(sqlRows, ptr.Interface(), 1)
	if err != nil {
		return err
	} else if n == int64(0) {
		return ErrNoRows
	} else {
		return nil
	}
}

func (s *rawSql) QueryRows(args interface{}, rows interface{}) (int64, error) {
	sqlRows, err := s.query(args)
	if err != nil {
		return int64(0), err
	}
	return s.parseRows(sqlRows, rows, -1)
}

func (s *rawSql) parseRows(sqlRows *sql.Rows, rowsPtr interface{}, limit int32) (int64, error) {
	val := reflect.ValueOf(rowsPtr)
	ind := reflect.Indirect(val)
	num := int64(0)
	if val.Kind() != reflect.Ptr || ind.Kind() != reflect.Slice {
		return num, errors.New("[database.sql] rows must be ptr slice")
	}

	elType := ind.Type().Elem()
	if elType.Kind() == reflect.Ptr {
		elType = elType.Elem()
	}

	if elType.Kind() != reflect.Map && (elType.Kind() != reflect.Struct || elType.String() == "time.Time") {
		return int64(0), errors.New("[database.sql] rows must be ptr slice of map or struct")
	}
	isStruct := (elType.Kind() == reflect.Struct)

	columns, err := sqlRows.Columns()
	if err != nil {
		return num, err
	}
	if isStruct {
		fields := s.getColumnFields(columns, elType)
		for sqlRows.Next() && limit != 0 {
			cols, err := s.scanRow(sqlRows, len(columns))
			if err != nil {
				return int64(0), err
			}
			if ind.Len() <= int(num) {
				elVal := reflect.New(ind.Type().Elem())
				ind = reflect.Append(ind, elVal.Elem())
			}
			elVal := ind.Index(int(num))
			if elVal.Kind() == reflect.Ptr && elVal.IsNil() {
				elVal.Set(reflect.New(elVal.Type().Elem()))
			}
			elVal = reflect.Indirect(elVal)
			for i, field := range fields {
				if field != nil {
					s.setFieldValue(elVal.Field((*field).Index[0]), cols[i])
				}
			}
			limit = limit - 1
			num = num + 1
		}
	} else {
		if elType.Key().Kind() != reflect.String {
			return int64(0), errors.New("[database.sql] map key must be string")
		}
		vType := elType.Elem()
		for sqlRows.Next() && limit != 0 {
			cols, err := s.scanRow(sqlRows, len(columns))
			if err != nil {
				return int64(0), err
			}
			if ind.Len() <= int(num) {
				elVal := reflect.New(ind.Type().Elem())
				ind = reflect.Append(ind, elVal.Elem())
			}
			elVal := ind.Index(int(num))
			if elVal.Kind() == reflect.Ptr && elVal.IsNil() {
				elVal.Set(reflect.New(elVal.Type().Elem()))
			}

			elVal = reflect.Indirect(elVal)

			if elVal.IsValid() {
				elVal.Set(reflect.MakeMap(elVal.Type()))
			}
			for i, column := range columns {
				vVal := reflect.New(vType)
				s.setFieldValue(vVal.Elem(), cols[i])
				elVal.SetMapIndex(reflect.ValueOf(column), vVal.Elem())
			}
			limit = limit - 1
			num = num + 1
		}
	}

	val.Elem().Set(ind)
	return num, nil
}

func (s *rawSql) setFieldValue(val reflect.Value, bytes sql.RawBytes) {
	switch val.Kind() {
	case reflect.Bool:
		var v = false
		if bytes != nil {
			v, _ = strconv.ParseBool(string(bytes))
		}
		val.SetBool(v)
	case reflect.String:
		val.SetString(string(bytes))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var v = int64(0)
		if bytes != nil {
			v, _ = strconv.ParseInt(string(bytes), 10, 64)
		}
		val.SetInt(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var v = uint64(0)
		if bytes != nil {
			v, _ = strconv.ParseUint(string(bytes), 10, 64)
		}
		val.SetUint(v)
	case reflect.Float32, reflect.Float64:
		var v = float64(0)
		if bytes != nil {
			v, _ = strconv.ParseFloat(string(bytes), 64)
		}
		val.SetFloat(v)
	case reflect.Struct:
		if _, ok := val.Interface().(time.Time); ok {
			str := string(bytes)
			if len(str) >= 19 {
				t, _ := time.Parse("2006-01-02 15:04:05", str[:19])
				val.Set(reflect.ValueOf(t))
			} else if len(str) >= 10 {
				t, _ := time.Parse("2006-01-02", str[:10])
				val.Set(reflect.ValueOf(t))
			}
		}
	case reflect.Interface:
		val.Set(reflect.ValueOf(bytes))
	}
}

func (s *rawSql) getColumnFields(columns []string, stype reflect.Type) []*reflect.StructField {
	fields := make([]*reflect.StructField, len(columns))
	for i, column := range columns {
		var field *reflect.StructField
		// column = strings.Replace(column, "_", "", -1)
		for j := 0; j < stype.NumField(); j++ {
			f := stype.Field(j)
			if f.Tag.Get("col") == column {
				field = &f
				break
			} else {
				if strings.ToLower(strings.Replace(column, "_", "", -1)) == strings.ToLower(f.Name) {
					field = &f
				}
			}
		}
		fields[i] = field
	}
	return fields
}

func (s *rawSql) scanRow(rows *sql.Rows, colNum int) ([]sql.RawBytes, error) {
	var (
		bytes = make([]sql.RawBytes, colNum)
		args  = make([]interface{}, colNum)
	)
	for i := 0; i < len(bytes); i++ {
		args[i] = &bytes[i]
	}
	err := rows.Scan(args...)
	return bytes, err
}

func (s *rawSql) Close() error {
	err := s.stmt.Close()
	if err == nil {
		s.stmt = nil
	}
	return err
}

func ReflectGetValue(root interface{}, property string) (interface{}, error) {
	rootValue := reflect.ValueOf(root)
	if rootValue.Kind() == reflect.Ptr {
		rootValue = rootValue.Elem()
	}
	var result interface{}
	var err error
	var value reflect.Value
	switch rootValue.Kind() {
	case reflect.Map:
		key := reflect.ValueOf(property)
		value = rootValue.MapIndex(key)
		break
	case reflect.Struct:
		value = rootValue.FieldByName(property)
		break
	default:
		err = errors.New(fmt.Sprintf("%v is not int (map, *map, struct, *struct)", root))
	}

	if err == nil {
		if value.IsValid() {
			switch value.Kind() {
			default:
				result = value.Interface()
			}
		} else {
			err = errors.New(property + " invalid")
		}
	}
	return result, err
}
