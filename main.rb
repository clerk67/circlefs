require 'bundler/setup'

require 'aws-sdk-cloudwatch'
require 'aws-sdk-s3'
require 'lru_redux'
require 'rfusefs'

class CircleFS < FuseFS::FuseDir

  def initialize(options)
    @bucket = options[:bucket]
    @region = options[:region]
    max_size = options[:cache_size] || 1000
    ttl = options[:cache_ttl] || 300
    @client = Aws::S3::Client.new({ region: @region })
    @cache = LruRedux::TTL::ThreadSafeCache.new(max_size, ttl)
  end

  def can_delete?(path)
    true
  end

  def can_mkdir?(path)
    true
  end

  def can_rmdir?(path)
    true
  end

  def can_write?(path)
    true
  end

  def contents(path)
    prefix = path.sub(/^\//, '')
    prefix << '/' if path != '/'
    resp = @client.list_objects_v2({
      bucket: @bucket,
      delimiter: '/',
      prefix: prefix,
    })
    files = resp.contents.map do |item|
      @cache[item.key] = {
        directory?: false,
        file?: true,
        size: item.size,
        times: Array.new(3, item.last_modified),
      }
      File.basename(item.key)
    end
    directories = resp.common_prefixes.map do |item|
      @cache[item.prefix.sub(/\/$/, '')] = {
        directory?: true,
        file?: false,
        size: 0,
        times: INIT_TIMES,
      }
      File.basename(item.prefix)
    end
    files + directories
  end

  def delete(path)
    resp = @client.delete_object({
      bucket: @bucket,
      key: path.sub(/^\//, ''),
    })
  end

  def directory?(path)
    if @cache.has_key?(path.sub(/^\//, ''))
      return @cache[path.sub(/^\//, '')][:directory?]
    end
    getattr(path)[:directory?]
  end

  def executable?(path)
    false
  end

  def file?(path)
    if @cache.has_key?(path.sub(/^\//, ''))
      return @cache[path.sub(/^\//, '')][:file?]
    end
    getattr(path)[:file?]
  end

  def getattr(path)
    begin
      resp = @client.head_object({
        bucket: @bucket,
        key: path.sub(/^\//, ''),
      })
      @cache[path.sub(/^\//, '')] = {
        directory?: false,
        file?: true,
        size: resp.content_length,
        times: Array.new(3, resp.last_modified),
      }
    rescue Aws::S3::Errors::NotFound
      {
        directory?: false,
        file?: false,
      }
    end
  end

  def mkdir(path)
    resp = @client.put_object({
      body: '',
      bucket: @bucket,
      key: path.sub(/^\//, ''),
    })
  end

  def read_file(path)
    resp = @client.get_object({
      bucket: @bucket,
      key: path.sub(/^\//, ''),
    })
    resp.body.read
  end

  def rename(from_path, to_path)
    false
  end

  def rmdir(path)
    resp = @client.delete_object({
      bucket: @bucket,
      key: path.sub(/^\//, ''),
    })
  end

  def size(path)
    if @cache.has_key?(path.sub(/^\//, ''))
      return @cache[path.sub(/^\//, '')][:size]
    end
    getattr(path)[:size]
  end

  def statistics(path)
    client = Aws::CloudWatch::Client.new({ region: @region })
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
      start_time: Time.now - 14 * 86400,
      end_time: Time.now,
    })
    [
      resp.metric_data_results[0].values[0].to_i,
      resp.metric_data_results[1].values[0].to_i,
      1024 ** 5,
      1024 ** 5,
    ]
  end

  def times(path)
    return INIT_TIMES if path == '/'
    if @cache.has_key?(path.sub(/^\//, ''))
      return @cache[path.sub(/^\//, '')][:times]
    end
    getattr(path)[:times]
  end

  def touch(path)
    write_to(path, '')
  end

  def write_to(path, str)
    resp = @client.put_object({
      body: str,
      bucket: @bucket,
      key: path.sub(/^\//, ''),
    })
  end
end

FuseFS.main(ARGV, [:bucket, :region]) do |options|
  CircleFS.new(options)
end
