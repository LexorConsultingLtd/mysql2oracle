require 'mysql2'

class MysqlTables
  attr_reader :table_names

  def initialize(host, userid, password, database)
    @@connection = Mysql2::Client.new(:host => host, :username => userid, :password => password, :database => database, :cache_rows => false, :cast_booleans => true)

    # Read table names
    result = MysqlTables.exec_sql('show tables')
    @table_names = result.map { |row| row.values.first }.sort
  end

  def self.exec_sql(sql)
    begin
      @@connection.query sql
    rescue Exception => e
      raise e, "Error executing: #{sql}"
    end
  end
end