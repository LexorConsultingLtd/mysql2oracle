require 'rubygems'
require 'mysql_tables'
require 'oracle_tables'
require 'pp'

src = MysqlTables.new('mysql.uat.serviceadvisory', 'service_advisory', 'service_advisory', 'service_advisory_uat')
dest = OracleTables.new('dev.bk', 'service_advisory_development', 'serviceadvisory')
oracle_table_names = src.table_names.map(&:upcase)

begin
  # Prepare for copy
  puts "Disabling constraints"
  dest.enable_constraints false, oracle_table_names

  # Copy data
  oracle_table_names.each do |table_name|
    table = dest.tables[table_name]
    if table
      puts "Copying data for #{table_name}"
      table.copy_data
    else
      puts "Unable to find destination table #{table_name}, skipping"
    end
  end
ensure
  # Re-enable constraints
  puts "Enabling constraints"
  dest.enable_constraints true, oracle_table_names
end

puts "Done"