require 'active_support'
require 'active_support/core_ext'
require 'aws-sdk-cloudwatch'

module CircusStatistics

  BUCKET_TOTAL_CAPACITY = 1.petabytes
  BUCKET_TOTAL_I_NODES = 1.petabytes

  READ_REQUEST_COST = 0.0004 / 1000
  WRITE_REQUEST_COST = 0.005 / 1000

  def statistics(path)
    record_request_statistics
    metrics = get_bucket_metrics
    [
      metrics[:bucket_size_bytes],
      metrics[:number_of_objects],
      BUCKET_TOTAL_CAPACITY,
      BUCKET_TOTAL_I_NODES,
    ]
  end

  def record_request_statistics
    total_requests = 0
    total_costs = 0
    @logger.info('========== S3 Request Statistics ==========')
    @logger.info("since #{@mounted_at.strftime('%FT%T.%6N')}")
    @counter.each do |key, value|
      case key
      when :COPY, :LIST, :POST, :PUT
        cost = value * WRITE_REQUEST_COST
        @logger.info(sprintf("%-6s %10d requests %12.7f USD", key, value, cost))
        total_requests += value
        total_costs += cost
      when :HEAD, :GET
        cost = value * READ_REQUEST_COST
        @logger.info(sprintf("%-6s %10d requests %12.7f USD", key, value, cost))
        total_requests += value
        total_costs += cost
      when :DELETE
        @logger.info(sprintf("%-6s %10d requests %12.7f USD", key, value, 0))
        total_requests += value
      end
    end
    @logger.info(sprintf("%-6s %10d requests %12.7f USD", 'TOTAL', total_requests, total_costs))
    @logger.info('===========================================')
  end

  def get_bucket_metrics
    client = Aws::CloudWatch::Client.new(region: @region)
    resp = client.get_metric_data({
      metric_data_queries: [{
        id: 'bucketSizeBytes',
        metric_stat: {
          metric: {
            namespace: 'AWS/S3',
            metric_name: 'BucketSizeBytes',
            dimensions: [{
              name: 'StorageType',
              value: 'StandardStorage',
            }, {
              name: 'BucketName',
              value: @bucket,
            }],
          },
          period: 86400,
          stat: 'Average',
        },
      }, {
        id: 'numberOfObjects',
        metric_stat: {
          metric: {
            namespace: 'AWS/S3',
            metric_name: 'NumberOfObjects',
            dimensions: [{
              name: 'StorageType',
              value: 'AllStorageTypes',
            }, {
              name: 'BucketName',
              value: @bucket,
            }],
          },
          period: 86400,
          stat: 'Average',
        },
      }],
      start_time: Time.now - 14.days,
      end_time: Time.now,
    })
    {
      bucket_size_bytes: resp.metric_data_results[0].values[0].to_i,
      number_of_objects: resp.metric_data_results[1].values[0].to_i,
    }
  rescue Aws::CloudWatch::Errors::ServiceError
    @logger.error($!.message)
    {
      bucket_size_bytes: 0,
      number_of_objects: 0,
    }
  end

end
