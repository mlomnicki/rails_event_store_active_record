require 'ruby_event_store/errors'

module RailsEventStoreActiveRecord
  class EventRepository
    WrongExpectedEventVersion = RubyEventStore::WrongExpectedEventVersion

    def initialize(adapter: Event)
      @adapter = adapter
    end
    attr_reader :adapter

    def create(event, stream_name, expected_version = :any)
      record = Event.create!(
        event_id:   event.event_id,
        event_type: event.class.name,
        data:       event.data,
        metadata:   event.metadata
      )
      case expected_version
      when :any  then create_any(record, stream_name)
      when :none then create_none(record, stream_name)
      else create_with_expected_version(record, stream_name, expected_version)
      end
      event
    end

    def delete_stream(stream_name)
      stream = Stream.find_by(name: stream_name) || return
      stream.events = []
      stream.destroy
    end

    def has_event?(event_id)
      adapter.exists?(event_id: event_id)
    end

    def last_stream_event(stream_name)
      stream = Stream.find_by(name: stream_name) || (return nil)
      build_event_entity(stream.events.last)
    end

    def read_events_forward(stream_name, start_event_id, count)
      stream = Stream.find_by(name: stream_name) || (return [])
      events = stream.events
      unless start_event_id.equal?(:head)
        starting_event = adapter.find_by(event_id: start_event_id)
        events = stream.events.where('id > ?', starting_event)
      end

      events.order('id ASC').limit(count)
        .map(&method(:build_event_entity))
    end

    def read_events_backward(stream_name, start_event_id, count)
      stream = Stream.find_by(name: stream_name) || (return [])
      events = stream.events
      unless start_event_id.equal?(:head)
        starting_event = adapter.find_by(event_id: start_event_id)
        events = events.where('id < ?', starting_event)
      end

      events.order('id DESC').limit(count)
        .map(&method(:build_event_entity))
    end

    def read_stream_events_forward(stream_name)
      stream = Stream.find_by(name: stream_name) || (return [])

      stream.events.order('id ASC')
        .map(&method(:build_event_entity))
    end

    def read_stream_events_backward(stream_name)
      stream = Stream.find_by(name: stream_name) || (return [])

      stream.events.order('id DESC')
        .map(&method(:build_event_entity))
    end

    def read_all_streams_forward(start_event_id, count)
      stream = adapter
      unless start_event_id.equal?(:head)
        starting_event = adapter.find_by(event_id: start_event_id)
        stream = stream.where('id > ?', starting_event)
      end

      stream.order('id ASC').limit(count)
        .map(&method(:build_event_entity))
    end

    def read_all_streams_backward(start_event_id, count)
      stream = adapter
      unless start_event_id.equal?(:head)
        starting_event = adapter.find_by(event_id: start_event_id)
        stream = stream.where('id < ?', starting_event)
      end

      stream.order('id DESC').limit(count)
        .map(&method(:build_event_entity))
    end

    private
    def build_event_entity(record)
      return nil unless record
      record.event_type.constantize.new(
        event_id: record.event_id,
        metadata: record.metadata,
        data: record.data
      )
    end

    def create_any(event, stream_name)
      # possible race condition. Should use UPSERT or ON CONFLICT IGNORE
      stream = Stream.find_by(name: stream_name) || create_stream(stream_name)
      stream.events << event
      Stream.where(name: stream_name).update_all("version = version + 1")
    end

    def create_none(event, stream_name)
      stream = create_stream(stream_name)
      stream.events << event
      cnt = Stream.where(name: stream_name, version: 0).update_all("version = version + 1")
      raise WrongExpectedEventVersion if cnt != 1
    rescue ActiveRecord::StatementInvalid, ActiveRecord::RecordNotUnique
      raise WrongExpectedEventVersion
    end

    def create_with_expected_version(event, stream_name, expected_version)
      stream = Stream.find_by(name: stream_name) || raise(WrongExpectedEventVersion)
      stream.events << event
      cnt = Stream.where(name: stream_name, version: expected_version)
                  .update_all(version: expected_version + 1)
      raise WrongExpectedEventVersion if cnt != 1
    end

    def create_stream(name)
      Stream.create!(name: name, version: 0)
    end
  end
end
