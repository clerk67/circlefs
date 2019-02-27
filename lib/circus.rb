require 'bundler/setup'

require 'active_support'
require 'active_support/core_ext'
require 'rfusefs'

require_relative 'circus/fusefs'
require_relative 'circus/parse_options'
require_relative 'circus/statistics'

class Circus < FuseFS::FuseDir

  CIRCUS_VERSION = '0.1.0'

  extend CircusOptionParser

  include CircusFuseDir
  include CircusStatistics

  def initialize(options)
    @logger = create_logger(options)
    total_memory = `free --bytes | awk '{ if (NR == 2) { print $2 }}'`
    @buffer_size = ((total_memory || 32.megabytes).to_i * 0.10).to_i

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
    @cache = create_cache(options)
    @counter = Hash.new(0)
    @mounted_at = Time.now
  end

  def create_logger(options)
    dev = options[:log_output] || '/dev/null'
    dev = STDOUT if dev == 'STDOUT'
    dev = STDERR if dev == 'STDERR'
    level = options[:log_level]
    Logger.new(dev, level: level, progname: self.class.name)
  end

  def create_cache(options)
    cache_ttl = options[:cache_ttl].to_i
    case options[:cache]
    when /^file:?(.*)$/
      cache_path = $1.presence || '/tmp/cache'
      ActiveSupport::Cache::FileStore.new(cache_path, expires_in: cache_ttl)
    when /^memcached:?(.*)$/
      servers = ($1.presence || 'localhost:11211').gsub(/(:\d+):/, '\1,').split(',')
      ActiveSupport::Cache::MemCacheStore.new(*servers, expires_in: cache_ttl)
    when /^memory:?(.*)$/
      size = ($1.presence || 32).to_i * 1024 * 1024
      ActiveSupport::Cache::MemoryStore.new(expires_in: cache_ttl, size: size)
    else
      STDERR.puts "#{self.class.name}: invalid cache driver (#{options[:cache]})"
      exit!
    end
  end

  def self.mount(root, mountpoint)
    FuseFS.mount(root, mountpoint)
  end

  def self.unmount(mountpoint = nil)
    FuseFS.unmount(mountpoint)
  end

end
