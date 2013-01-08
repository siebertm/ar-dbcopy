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
require 'active_record'
require 'logger'

class ARDBCopy
  attr_accessor :config, :tables

  def initialize(config_file, opts={})
    @copy_schema = opts[:copy_schema]
    @config = YAML.load_file(config_file)

    ActiveRecord::Base.logger ||= Logger.new(nil)

    ActiveRecord::Base.establish_connection(@config["source"])
    @tables = ActiveRecord::Base.connection.tables.reject { |m| m == "schema_migrations" }
  end

  def run!
    copy_schema if @copy_schema
    copy_data
  end

  def copy_schema
    io = StringIO.new

    ActiveRecord::SchemaDumper.dump(ActiveRecord::Base.connection, io) # dump the schema from the source database
    io.rewind

    ActiveRecord::Base.establish_connection(config['target'])
    eval(io.read)

    ActiveRecord::Base.establish_connection(config["source"])
  end

  def copy_data
    tables.each do |table_name|
      source_name = "#{table_name.classify}Source"
      target_name = "#{table_name.classify}Target"

      source_model_tmp = Class.new(ActiveRecord::Base) do
        self.table_name = table_name
        self.inheritance_column = :_type_disabled
      end
      source_model = Object.const_set(source_name, source_model_tmp)
      source_model.establish_connection(config["source"])

      target_model_tmp = Class.new(ActiveRecord::Base) do
        self.table_name = table_name
        self.inheritance_column = :_type_disabled
      end
      target_model = Object.const_set(target_name, target_model_tmp)
      target_model.establish_connection(config["target"])

      target_model.delete_all

      puts "Copying #{table_name} (#{source_model.count} lines)..."

      i = 0
      source_model.find_in_batches(:batch_size => 10_000) do |src_batch|
        target_model.transaction do
          src_batch.each do |src_inst|
            puts " # #{table_name}: #{src_inst.id}"
            dst_inst = target_model.new(src_inst.attributes)
            dst_inst.id = src_inst.id
            dst_inst.save!
            i += 1
          end

          puts i
        end
      end

      source_model.remove_connection
      target_model.remove_connection
    end
  end
end

if __FILE__ == $0
  copy = ARDBcopy.new(ARGV[0])
  copy.copy_schema
  copy.copy_data
end
