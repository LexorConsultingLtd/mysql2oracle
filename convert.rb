require 'rubygems'
require 'oracle'

dest = Oracle.new('instance', 'test', 'tester')
dest.enable_constraints false
dest.clear_table_data
dest.enable_constraints true
