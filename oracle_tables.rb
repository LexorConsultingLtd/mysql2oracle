require 'oci8'
require 'pp'

class OracleTables
  attr_reader :tables, :constraints

  def initialize(instance_name, userid, password)
    @tables = {}
    @constraints = {}
    @cn = OCI8.new(userid, password, instance_name)

    # Read tables and store column info
    @cn.exec('select table_name from user_tables order by table_name') do |row|
      table_name = row[0]
      @tables[table_name] = {:name => table_name, :columns => {}, :constraints => {}}
      @cn.describe_table(table_name).columns.each do |col|
        attrs = {}
        [:name, :type_string, :data_type, :nullable?, :precision, :scale, :char_size, :data_size].each{|val| attrs[val] = col.send(val)}
        @tables[table_name][:columns][col.name] = attrs
      end
    end

    # Read constraints
    @cn.exec('select table_name, constraint_name, constraint_type from user_constraints order by constraint_name') do |row|
      table_name, constraint_name, constraint_type = row
      @tables[table_name][:constraints][constraint_name] = {
        :table_name => table_name,
        :constraint_name => constraint_name,
        :type => case constraint_type
          when 'C'
            :check
          when 'P'
            :primary_key
          when 'R'
            :foreign_key
          else
            raise "Unexpected constraint type #{constraint_type} on table #{table_name}, constraint #{constraint_name}"
          end
      }
    end
  end

  # Order for enabling constraints. Reverse this when disabling.
  CONSTRAINT_ENABLE_ORDER = [:check, :primary_key, :foreign_key]

  # List of all constraints
  def all_constraints
    tables.map{|name, info| info[:constraints].map(&:last)}.flatten
  end

  # Enable or disable all constraints
  def enable_constraints(enable = true, *tables)
    constraints = all_constraints.group_by{|c| c[:type]}
    (enable ? CONSTRAINT_ENABLE_ORDER : CONSTRAINT_ENABLE_ORDER.reverse).each do |type|
      constraints[type].each do |constraint|
        if tables.empty? || tables.include?(constraint[:table_name])
          sql = "alter table #{constraint[:table_name]} #{enable ? :enable : :disable} constraint #{constraint[:constraint_name]}"
          exec_sql sql
        end
      end if constraints[type]
    end
  end

  def clear_table_data(*tables_to_clear)
    tables.each do |name, info|
      if tables_to_clear.empty? || tables_to_clear.include?(name)
        sql = "delete from #{name}"
        exec_sql sql
      end
    end
  end

  def copy_table(mysql_connection, mysql_table_name)
    return unless table = tables[mysql_table_name.upcase]
    rows = mysql_connection.read_table(mysql_table_name)
    column_names = table[:columns].map(&:first).sort
    sql = "insert into #{table[:name]}(#{column_names.join(', ')}) values (#{column_names.map{|c| ':' + c.downcase}.join(', ')})"
    rows.each do |row|
      values = column_names.map{|c| row[c.downcase]}
      pp exec_sql sql, *values
    end
  end

  def exec_sql(sql, *params)
    begin
      pp sql
      pp params
      @cn.exec sql, *params
    rescue Exception => e
      raise e, "Error executing: #{sql}. params are: [#{params.join(', ')}]"
    end
  end
end