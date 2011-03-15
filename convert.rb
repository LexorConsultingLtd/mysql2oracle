require 'rubygems'
require 'mysql_tables'
require 'oracle_tables'

src = MysqlTables.new('localhost', 'test', 'test', 'test')
dest = OracleTables.new('dev', 'test', 'test')

oracle_table_names = src.tables.map(&:upcase)
dest.enable_constraints false, *oracle_table_names
dest.clear_table_data *oracle_table_names
src.tables.each do |table_name|
  dest.copy_table src, table_name
end
dest.enable_constraints true, *oracle_table_names
