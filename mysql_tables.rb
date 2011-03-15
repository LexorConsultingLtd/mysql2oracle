require 'mysql2'
require 'pp'

class MysqlTables
  attr_reader :tables, :connection

  def initialize(host, userid, password, database)
    @connection = Mysql2::Client.new(:host => host, :username => userid, :password => password, :database => database, :cache_rows => false, :cast_booleans => true)

    # Read table names
    result = exec_sql('show tables')
    @tables = result.map{|row| row.values.first}.sort
  end

  def read_table(table_name)
    @connection.query "select * from #{table_name} order by 1"
  end

  private

  def exec_sql(sql)
    begin
      pp sql
      @connection.query sql
    rescue Exception => e
      raise e, "Error executing: #{sql}"
    end
  end
end