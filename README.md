Experimental Custom Kafka implementation

Persistent storage

/baseDir/
  /topic/
    /partition-0/
      data.log    # Actual message data
      index.idx   # Index for quick offset lookup
      offset.meta # Current offset tracking
