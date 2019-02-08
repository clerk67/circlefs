require 'bundler/setup'

require 'active_support'
require 'active_support/core_ext'
require 'aws-sdk-cloudwatch'
require 'aws-sdk-s3'
require 'rfusefs'

class Circus < FuseFS::FuseDir

  def initialize(options)
    logdev = options[:log_output] || '/dev/null'
    logdev = STDOUT if logdev == 'STDOUT'
    logdev = STDERR if logdev == 'STDERR'
    level = options[:log_level]
    @logger = Logger.new(logdev, level: level, progname: self.class.name)

    @bucket = options[:bucket]
    @region = options[:region]
    if options[:access_key_id] && options[:secret_access_key] then
      credentials = Aws::Credentials.new(options[:access_key_id], options[:secret_access_key])
      Aws.config.update({ credentials: credentials })
    end
    @client = Aws::S3::Client.new(region: @region || 'us-east-1')
    unless @region then
      resp = @client.get_bucket_location({ bucket: @bucket })
      @region = resp.location_constraint
      @client = Aws::S3::Client.new(region: @region)
    end

    cache_ttl = options[:cache_ttl].to_i
    case options[:cache]
    when /^file:?(.*)$/
      cache_path = $1.presence || '/tmp/cache'
      @cache = ActiveSupport::Cache::FileStore.new(cache_path, expires_in: cache_ttl)
    when /^memcached:?(.*)$/
      servers = ($1.presence || 'localhost:11211').gsub(/(:\d+):/, '\1,').split(',')
      @cache = ActiveSupport::Cache::MemCacheStore.new(*servers, expires_in: cache_ttl)
    when /^memory:?(.*)$/
      size = ($1.presence || 32).to_i * 1024 * 1024
      @cache = ActiveSupport::Cache::MemoryStore.new(expires_in: cache_ttl, size: size)
    else
      puts "#{self.class.name}: invalid cache driver (#{options[:cache]})"
      exit!
    end
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
    files.reject! { |item| item.key == prefix } if delimiter == '/'
    files.map! do |item|
      @cache.write(item.key, {
        directory?: false,
        file?: true,
        size: item.size,
        times: Array.new(3, item.last_modified),
      })
      delimiter == '/' ? File.basename(item.key) : item.key
    end
    directories = resp.common_prefixes.map do |item|
      @cache.write(item.prefix.sub(/\/$/, ''), {
        directory?: true,
        file?: false,
        size: 0,
        times: INIT_TIMES,
      })
      delimiter == '/' ? File.basename(item.prefix) : item.prefix
    end
    files + directories
  end

  def delete(path)
    key = path.sub(/^\//, '')
    key << '/' if directory?(path)
    @logger.debug("DELETE s3://#{@bucket}/#{key}")
    resp = @client.delete_object({
      bucket: @bucket,
      key: key,
    })
  end

  def directory?(path)
    getattr(path)[:directory?]
  end

  def executable?(path)
    false
  end

  def file?(path)
    getattr(path)[:file?]
  end

  def getattr(path)
    @cache.fetch(path.sub(/^\//, '')) do
      begin
        @logger.debug("HEAD   s3://#{@bucket}/#{path.sub(/^\//, '')}")
        resp = @client.head_object({
          bucket: @bucket,
          key: path.sub(/^\//, ''),
        })
        {
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
  end

  def mkdir(path)
    @logger.debug("PUT    s3://#{@bucket}/#{path.sub(/^\//, '')}/")
    resp = @client.put_object({
      body: '',
      bucket: @bucket,
      key: "#{path.sub(/^\//, '')}/",
    })
    @cache.write(path.sub(/^\//, ''), {
      directory?: true,
      file?: false,
      size: 0,
      times: INIT_TIMES,
    })
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
    if directory?(from_path) then
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
    getattr(path)[:size]
  end

  def statistics(path)
    client = Aws::CloudWatch::Client.new
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
    [
      resp.metric_data_results[0].values[0].to_i,
      resp.metric_data_results[1].values[0].to_i,
      1.petabytes,
      1.petabytes,
    ]
  end

  def times(path)
    return INIT_TIMES if path == '/'
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

