require 'bundler/setup'

require 'aws-sdk-cloudwatch'
require 'aws-sdk-s3'
require 'lru_redux'
require 'rfusefs'

class Circus < FuseFS::FuseDir

  def initialize(options)
    @bucket = options[:bucket]
    @region = options[:region]
    max_size = options[:cache_size] || 1000
    ttl = options[:cache_ttl] || 300
    @client = Aws::S3::Client.new({ region: @region })
    @cache = LruRedux::TTL::ThreadSafeCache.new(max_size, ttl)
    @logger = Logger.new(STDERR, progname: self.class.name)
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

  def contents(path, delimiter = '/')
    prefix = path.sub(/^\//, '')
    prefix << '/' unless path == '/'
    @logger.debug("LIST   s3://#{@bucket}/#{prefix}")
    resp = @client.list_objects_v2({
      bucket: @bucket,
      delimiter: delimiter,
      prefix: prefix,
    })
    files = resp.contents
    if delimiter == '/'
      files.reject! { |item| item.key == prefix }
    end
    files.map! do |item|
      @cache[item.key] = {
        directory?: false,
        file?: true,
        size: item.size,
        times: Array.new(3, item.last_modified),
      }
      delimiter == '/' ? File.basename(item.key) : item.key
    end
    directories = resp.common_prefixes.map do |item|
      @cache[item.prefix.sub(/\/$/, '')] = {
        directory?: true,
        file?: false,
        size: 0,
        times: INIT_TIMES,
      }
      delimiter == '/' ? File.basename(item.prefix) : item.prefix
    end
    files + directories
  end

  def delete(path)
    key = path.sub(/^\//, '')
    if @cache.has_key?(path.sub(/^\//, ''))
      key << '/' if @cache[path.sub(/^\//, '')][:directory?]
    end
    @logger.debug("DELETE s3://#{@bucket}/#{key}")
    resp = @client.delete_object({
      bucket: @bucket,
      key: key,
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
      @logger.debug("HEAD   s3://#{@bucket}/#{path.sub(/^\//, '')}")
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
    @logger.debug("PUT    s3://#{@bucket}/#{path.sub(/^\//, '')}/")
    resp = @client.put_object({
      body: '',
      bucket: @bucket,
      key: "#{path.sub(/^\//, '')}/",
    })
    @cache[path.sub(/^\//, '')] = {
      directory?: true,
      file?: false,
      size: 0,
      times: INIT_TIMES,
    }
  end

  def read_file(path)
    @logger.debug("GET    s3://#{@bucket}/#{path.sub(/^\//, '')}")
    resp = @client.get_object({
      bucket: @bucket,
      key: path.sub(/^\//, ''),
    })
    resp.body.read
  end

  def rename(from_path, to_path)
    if directory?(from_path)
      objects = contents(from_path, '')
    else
      objects = [from_path.sub(/^\//, '')]
    end
    objects.each do |item|
      key = item.sub(from_path.sub(/^\//, ''), to_path.sub(/^\//, ''))
      @logger.debug("COPY   s3://#{@bucket}/#{item} -> s3://#{@bucket}/#{key}")
      @client.copy_object({
        bucket: @bucket,
        copy_source: "/#{@bucket}/#{item}",
        key: key,
      })
    end
    @logger.debug("DELETE #{objects.map { |item| "s3://#{@bucket}/#{item}" }.join(' ')}")
    @client.delete_objects({
      bucket: @bucket,
      delete: {
        objects: objects.map { |item| { key: item } },
      },
    })
  end

  def rmdir(path)
    @logger.debug("DELETE s3://#{@bucket}/#{path.sub(/^\//, '')}/")
    resp = @client.delete_object({
      bucket: @bucket,
      key: "#{path.sub(/^\//, '')}/",
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
    @logger.debug("PUT    s3://#{@bucket}/#{path.sub(/^\//, '')}")
    resp = @client.put_object({
      body: str,
      bucket: @bucket,
      key: path.sub(/^\//, ''),
    })
  end
end

FuseFS.main(ARGV, [:bucket, :region]) do |options|
  Circus.new(options)
end
