require 'spec_helper'
require 'ruby_event_store'
require 'ruby_event_store/spec/event_repository_lint'

module RailsEventStoreActiveRecord
  describe EventRepository do
    TestDomainEvent = Class.new(RubyEventStore::Event)
    let(:repository) { EventRepository.new }

    it 'just created is empty' do
      expect(repository.read_all_streams_forward(:head, 1)).to be_empty
    end

    it 'what you get is what you gave' do
      created = repository.create(event = TestDomainEvent.new, 'stream')
      expect(created.object_id).to eq event.object_id
    end

    it 'created event is stored in given stream' do
      expected_event = TestDomainEvent.new(data: {})
      created = repository.create(expected_event, 'stream')
      expect(created).to eq(expected_event)
      expect(repository.read_all_streams_forward(:head, 1).first).to eq(expected_event)
      expect(repository.read_stream_events_forward('stream').first).to eq(expected_event)
      expect(repository.read_stream_events_forward('other_stream')).to be_empty
    end

    it 'data attributes are retrieved' do
      event = TestDomainEvent.new(data: { order_id: 3 })
      repository.create(event, 'stream')
      retrieved_event = repository.read_all_streams_forward(:head, 1).first
      expect(retrieved_event.data[:order_id]).to eq(3)
    end

    it 'metadata attributes are retrieved' do
      event = TestDomainEvent.new(metadata: { request_id: 3 })
      repository.create(event, 'stream')
      retrieved_event = repository.read_all_streams_forward(:head, 1).first
      expect(retrieved_event.metadata[:request_id]).to eq(3)
    end

    it 'does not have deleted streams' do
      repository.create(TestDomainEvent.new, 'stream')
      repository.create(TestDomainEvent.new, 'other_stream')

      expect(repository.read_stream_events_forward('stream').count).to eq 1
      expect(repository.read_stream_events_forward('other_stream').count).to eq 1
      expect(repository.read_all_streams_forward(:head, 10).count).to eq 2

      repository.delete_stream('stream')
      expect(repository.read_stream_events_forward('stream')).to be_empty
      expect(repository.read_stream_events_forward('other_stream').count).to eq 1
      expect(repository.read_all_streams_forward(:head, 10).count).to eq 2
    end

    it 'has or has not domain event' do
      repository.create(TestDomainEvent.new(event_id: 'just an id'), 'stream')

      expect(repository.has_event?('just an id')).to be_truthy
      expect(repository.has_event?('any other id')).to be_falsey
    end

    it 'knows last event in stream' do
      repository.create(TestDomainEvent.new(event_id: 'event 1'), 'stream')
      repository.create(TestDomainEvent.new(event_id: 'event 2'), 'stream')

      expect(repository.last_stream_event('stream')).to eq(TestDomainEvent.new(event_id: 'event 2'))
      expect(repository.last_stream_event('other_stream')).to be_nil
    end

    it 'reads batch of events from stream forward & backward' do
      event_ids = (1..10).to_a.map(&:to_s)
      repository.create(TestDomainEvent.new(event_id: '21'), 'other_stream')
      event_ids.each do |id|
        repository.create(TestDomainEvent.new(event_id: id), 'stream')
      end
      repository.create(TestDomainEvent.new(event_id: '22'), 'other_stream')

      expect(repository.read_events_forward('stream', :head, 3)).to eq ['1','2','3'].map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_events_forward('stream', :head, 100)).to eq event_ids.map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_events_forward('stream', '5', 4)).to eq ['6','7','8','9'].map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_events_forward('stream', '5', 100)).to eq ['6','7','8','9','10'].map{|x| TestDomainEvent.new(event_id: x)}

      expect(repository.read_events_backward('stream', :head, 3)).to eq ['10','9','8'].map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_events_backward('stream', :head, 100)).to eq event_ids.reverse.map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_events_backward('stream', '5', 4)).to eq ['4','3','2','1'].map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_events_backward('stream', '5', 100)).to eq ['4','3','2','1'].map{|x| TestDomainEvent.new(event_id: x)}
    end


    it 'reads all stream events forward & backward' do
      repository.create(TestDomainEvent.new(event_id: '1'), 'stream')
      repository.create(TestDomainEvent.new(event_id: '2'), 'other_stream')
      repository.create(TestDomainEvent.new(event_id: '3'), 'stream')
      repository.create(TestDomainEvent.new(event_id: '4'), 'other_stream')
      repository.create(TestDomainEvent.new(event_id: '5'), 'other_stream')

      expect(repository.read_stream_events_forward('stream')).to eq ['1','3'].map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_stream_events_backward('stream')).to eq ['3','1'].map{|x| TestDomainEvent.new(event_id: x)}
    end

    it 'reads batch of events from all streams forward & backward' do
      event_ids = (1..10).to_a.map(&:to_s)
      event_ids.each do |id|
        repository.create(TestDomainEvent.new(event_id: id), SecureRandom.uuid)
      end

      expect(repository.read_all_streams_forward(:head, 3)).to eq ['1','2','3'].map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_all_streams_forward(:head, 100)).to eq event_ids.map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_all_streams_forward('5', 4)).to eq ['6','7','8','9'].map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_all_streams_forward('5', 100)).to eq ['6','7','8','9','10'].map{|x| TestDomainEvent.new(event_id: x)}

      expect(repository.read_all_streams_backward(:head, 3)).to eq ['10','9','8'].map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_all_streams_backward(:head, 100)).to eq event_ids.reverse.map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_all_streams_backward('5', 4)).to eq ['4','3','2','1'].map{|x| TestDomainEvent.new(event_id: x)}
      expect(repository.read_all_streams_backward('5', 100)).to eq ['4','3','2','1'].map{|x| TestDomainEvent.new(event_id: x)}
    end

    it 'fails if expected version does not match' do
      repository.create(TestDomainEvent.new(event_id: '1'), 'stream', :none)
      repository.create(TestDomainEvent.new(event_id: '2'), 'stream', 1)
      repository.create(TestDomainEvent.new(event_id: '3'), 'stream', 2)

      expect do
        repository.create(TestDomainEvent.new(event_id: '4'), 'stream', 2)
      end.to raise_error(RubyEventStore::WrongExpectedEventVersion)
    end

    it 'fails if stream is empty but a particular version is expected' do
      expect do
        repository.create(TestDomainEvent.new(event_id: '2'), 'stream', 1)
      end.to raise_error(RubyEventStore::WrongExpectedEventVersion)
    end

    it 'fails if there are events in stream but it is expected to be empty' do
      repository.create(TestDomainEvent.new(event_id: '1'), 'stream', :none)
      expect do
        repository.create(TestDomainEvent.new(event_id: '2'), 'stream', :none)
      end.to raise_error(RubyEventStore::WrongExpectedEventVersion)
    end

    it 'can ignore stream version' do
      repository.create(TestDomainEvent.new(event_id: '1'), 'stream', :any)
      expect do
        repository.create(TestDomainEvent.new(event_id: '2'), 'stream', :any)
      end.not_to raise_error
    end

    it 'threads OMG' do
      threads_count = 5
      do_wait       = true
      failures      = 0

      repository.create(TestDomainEvent.new(event_id: SecureRandom.uuid), 'stream', :none)
      threads = threads_count.times.map do |id|
        Thread.new do
          nil while do_wait
          ActiveRecord::Base.connection_pool.with_connection do
            begin
              repository.create(TestDomainEvent.new(event_id: SecureRandom.uuid), 'stream', 1)
            rescue RubyEventStore::WrongExpectedEventVersion
              failures += 1
            end
          end
        end
      end
      do_wait = false
      threads.each(&:join)
      expect(failures).to eq(threads_count - 1)
    end
  end
end
