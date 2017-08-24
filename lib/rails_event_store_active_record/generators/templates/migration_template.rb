class CreateEventStoreEvents < ActiveRecord::Migration<%= migration_version %>
  def change
    create_table(:event_store_events) do |t|
      t.string      :event_type,  null: false
      t.string      :event_id,    null: false
      t.text        :metadata
      t.text        :data,        null: false
      t.datetime    :created_at,  null: false
    end

    create_table(:event_store_streams) do |t|
      t.string   :name,        null: false
      t.string   :version,     null: false
      t.datetime :created_at,  null: false
    end

    create_table(:event_store_streams_events, id: false) do |t|
      t.integer :stream_id, null: false
      t.integer :event_id,  null: false
    end

    add_index :event_store_streams, :name, unique: true
    add_index :event_store_streams_events, [:stream_id, :event_id], unique: true
    add_index :event_store_events, :created_at
    add_index :event_store_events, :event_type
    add_index :event_store_events, :event_id, unique: true
  end
end
