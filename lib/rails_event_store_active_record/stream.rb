require 'active_record'

module RailsEventStoreActiveRecord
  class Stream < ::ActiveRecord::Base
    self.table_name = 'event_store_streams'

    has_and_belongs_to_many :events, join_table:  'event_store_streams_events'
  end
end
