require 'oci8'
require 'mysql_tables'

class OracleColumn
  NULL = 'null'
  GEOM_TYPE = 'MDSYS.SDO_GEOMETRY'
  ATTRIBUTES = [:name, :type_string, :data_type, :precision, :scale, :char_size, :data_size]
  attr_reader :table, :nullable, *ATTRIBUTES

  def initialize(parent_table, column_info)
    @table = parent_table
    @nullable = column_info.nullable? # Can't call an attribute "nullable?" otherwise it would be in the array too
    ATTRIBUTES.each { |attr| instance_variable_set "@#{attr}", column_info.send(attr) }
  end

  def select_text
    mysql_name = name.downcase
    #pp data_type
    case data_type
      when :named_type
        case type_string
          when 'MDSYS.SDO_GEOMETRY'
            "AsWKT(#{mysql_name}) as #{mysql_name}"
          else
            puts "Unknown named type/object column type #{type_string} for #{table.name}.#{name}, selecting null"
            NULL
        end
      else
        mysql_name
    end
  end

  def insert_text
    #pp data_type
    case data_type
      when :named_type
        case type_string
          when GEOM_TYPE
            "#{GEOM_TYPE}(':#{name.downcase}')"
          else
            puts "Unknown named type/object column type #{type_string} for #{table.name}.#{name}"
        end
      else
        ":#{name.downcase}"
    end
  end

  def insert_value_text(value)
    return NULL unless value
    #pp data_type
    case data_type
      when :char, :varchar2
        "'#{value.gsub "'", "''"}'"
      when :date
        "to_date('#{value.strftime("%Y-%m-%d %H:%M:%S")}', 'yyyy-mm-dd hh24:mi:ss')"
      when :named_type
        case type_string
          when GEOM_TYPE
            "#{GEOM_TYPE}('#{value}')"
          else
            puts "Unknown named type/object column type #{type_string} for #{table.name}.#{name}, inserting null"
            NULL
        end
      else
        value
    end
  end
end

class OracleConstraint
  # Order for enabling constraints. Reverse this when disabling.
  ENABLE_ORDER = [:check, :primary_key, :foreign_key]
  attr_reader :name, :type, :table

  def initialize(parent_table, constraint_row)
    name, type = constraint_row
    @table = parent_table
    @name = name
    @type =
        case type
          when 'C'
            :check
          when 'P'
            :primary_key
          when 'R'
            :foreign_key
          else
            raise "Unexpected constraint type #{type} on table #{table.name}, constraint #{name}"
        end
  end
end

class OracleTrigger
  attr_reader :name, :table

  def initialize(parent_table, trigger_row)
    @table = parent_table
    @name = trigger_row[0]
  end
end

class OracleTable
  CONSTRAINT_SQL = 'select constraint_name, constraint_type from user_constraints where table_name = :table_name'
  TRIGGER_SQL = 'select trigger_name from user_triggers where table_name = :table_name'
  BATCH_SIZE = 500
  attr_reader :name, :columns, :constraints, :triggers

  def initialize(table_name)
    @name = table_name
    @columns = OracleTables.connection.describe_table(table_name).columns.map { |col| OracleColumn.new(self, col) }
    @constraints = []
    OracleTables.exec_sql(CONSTRAINT_SQL, table_name) { |row| @constraints << OracleConstraint.new(self, row) }
    @triggers = []
    OracleTables.exec_sql(TRIGGER_SQL, table_name) { |row| @triggers << OracleTrigger.new(self, row) }
  end

  def clear_data
    OracleTables.exec_sql "delete from #{name}"
  end

  def copy_data
    puts "  disabling triggers"
    enable_triggers false

    puts "  clearing data"
    clear_data

    puts "  copying data"
    start_time = Time.now
    num_rows = 0
    read_sql = "select #{columns.map(&:select_text).join(', ')} from #{name.downcase} order by 1"
    insert_sql = "insert into #{name}(#{columns.map(&:name).join(', ')}) values(#{columns.map(&:insert_text).join(', ')})"
    OracleTables.connection.autocommit = false
    MysqlTables.exec_sql(read_sql).each do |row|
      values = columns.map{|c| row[c.name.downcase] || ''}
      OracleTables.exec_sql insert_sql, *values
      num_rows += 1
      if num_rows % BATCH_SIZE == 0
        write_stats start_time, num_rows
        OracleTables.connection.commit
      end
    end
    OracleTables.connection.commit
    write_stats start_time, num_rows

    puts "  enabling triggers"
    enable_triggers true
  end

  # Enable or disable triggers on a table
  def enable_triggers(enable = true)
    triggers.each do |trigger|
      sql = "alter trigger #{trigger.name} #{enable ? :enable : :disable}"
      OracleTables.exec_sql sql
    end
  end

  private

  def write_stats(start_time, row_count)
    puts "  processed #{format_num(row_count)} rows (#{"%.2f" % (row_count/(Time.now-start_time))}/s)"
  end

def format_num(num, delim = ',')
  num.to_s.reverse.gsub(%r{([[:digit:]]{3})(?=[[:digit:]])(?![[:digit:]]*\.)}, "\\1#{delim}").reverse
end


end

class OracleTables
  attr_reader :tables, :constraints

  def initialize(instance_name, userid, password)
    @@connection = OCI8.new(userid, password, instance_name)

    # Read table metadata
    @tables = {}
    @@connection.exec('select table_name from user_tables order by 1') do |row|
      table_name = row[0]
      @tables[table_name] = OracleTable.new(table_name)
    end
  end

  # List of all constraints
  def all_constraints(table_filter=nil)
    constraints = tables.values.map(&:constraints).flatten
    constraints.delete_if{|c|!table_filter.include?(c.table.name)} if table_filter
    constraints
  end

  # Enable or disable constraints
  def enable_constraints(enable = true, table_filter=nil)
    constraints_by_type = all_constraints(table_filter).group_by { |c| c.type }
    (enable ? OracleConstraint::ENABLE_ORDER : OracleConstraint::ENABLE_ORDER.reverse).each do |type|
      constraints_by_type[type].each do |constraint|
        sql = "alter table #{constraint.table.name} #{enable ? :enable : :disable} constraint #{constraint.name}"
        OracleTables.exec_sql sql
      end if constraints_by_type[type]
    end
  end

  def self.connection
    @@connection
  end

  def self.exec_sql(sql, *params, &block)
    begin
#      pp sql
#      pp params
      @@connection.exec sql, *params, &block
    rescue Exception => e
      raise e, "Error executing: #{sql}. params are: [#{params.join(', ')}]"
    end
  end

end