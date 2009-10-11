#!/usr/bin/env ruby
#
# Simple script to copy data from one database to another.
# This script can be used to migrate from one Database system
# to another one, e.q. when the SQL dumps are not compatible.
# Database agnosticism is achieved by using ActiveRecord.
#
# The script just defines 2 connections, looks at the tables,
# creates ActiveRecord model classes on-the-fly and copies over
# the data.
#
# Configure the databases using the database.yml.example as a
# template (the format is the same as Rails's database.yml)
#
# Then, run the script. The script only copies data, not structure,
# so make sure that you created the schema  and all tables in
# the destination database already (use rake db:schema:load)
#
require "rubygems"
require 'active_record'

$config = YAML.load_file(File.join(File.dirname(__FILE__), 'database.yml'))

ActiveRecord::Base.logger = Logger.new(nil)

class SourceDB < ActiveRecord::Base
  establish_connection $config["source"]
end

class DestDB < ActiveRecord::Base
  establish_connection $config["target"]
end


$models = SourceDB.connection.tables.reject { |m| m == "schema_migrations" }.map(&:classify)

module Source; end
module Dest; end

$models.each do |model|
  eval "class Source::#{model} < SourceDB; set_inheritance_column :not_sti; set_table_name :#{model.tableize}; end"
  eval "class Dest::#{model} < DestDB; set_inheritance_column :not_sti; set_table_name :#{model.tableize}; end"

  src = "Source::#{model}".constantize
  dst = "Dest::#{model}".constantize

  dst.delete_all

  puts "Copying #{model} (#{src.count} instances)..."

  i = 0
  src.find_in_batches(:batch_size => 10_000) do |src_batch|
    dst.transaction do
      src_batch.each do |src_inst|
        dst_inst = dst.new(src_inst.attributes)
        dst_inst.id = src_inst.id
        dst_inst.save!
        i += 1
      end

      puts i
    end
  end
end

