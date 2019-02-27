require 'bundler/setup'

require 'active_support'
require 'active_support/core_ext'
require 'aws-sdk-s3'

module CircusFuseDir

  MAX_THREADS = 8
  MULTIPART_CHUNKSIZE = 32.megabytes

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
    @counter[:LIST] += 1
    @logger.info("LIST   s3://#{@bucket}/#{prefix}")
    resp = @client.list_objects_v2({
      bucket: @bucket,
      delimiter: delimiter,
      prefix: prefix,
    })
    files = resp.contents
    files.reject! { |item| item.key == prefix } if delimiter == '/'
    files.map! do |item|
      @cache.write("#{@bucket}/#{item.key}", {
        directory?: false,
        file?: true,
        size: item.size,
        times: Array.new(3, item.last_modified),
      })
      delimiter == '/' ? File.basename(item.key) : item.key
    end
    directories = resp.common_prefixes.map do |item|
      @cache.write("#{@bucket}/#{item.prefix.sub(/\/$/, '')}", {
        directory?: true,
        file?: false,
        size: 0,
        times: FuseFS::FuseDir::INIT_TIMES,
      })
      delimiter == '/' ? File.basename(item.prefix) : item.prefix
    end
    files + directories
  rescue Aws::S3::Errors::ServiceError
    @logger.error($!.message)
    nil
  end

  def delete(path)
    key = path.sub(/^\//, '')
    key << '/' if directory?(path)
    @counter[:DELETE] += 1
    @logger.info("DELETE s3://#{@bucket}/#{key}")
    @client.delete_object({
      bucket: @bucket,
      key: key,
    })
    @cache.delete("#{@bucket}/#{key}")
  rescue Aws::S3::Errors::ServiceError
    @logger.error($!.message)
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
    key = path.sub(/^\//, '')
    if @cache.exist?("#{@bucket}/#{key}") then
      return @cache.read("#{@bucket}/#{key}")
    end
    @counter[:HEAD] += 1
    @logger.info("HEAD   s3://#{@bucket}/#{key}")
    resp = @client.head_object({
      bucket: @bucket,
      key: key,
    })
    attributes = {
      directory?: false,
      file?: true,
      size: resp.content_length,
      times: Array.new(3, resp.last_modified),
    }
    @cache.write("#{@bucket}/#{key}", attributes)
    attributes
  rescue Aws::S3::Errors::NotFound
    contents = contents(File.dirname(path))
    if @cache.exist?("#{@bucket}/#{key}")
      return @cache.read("#{@bucket}/#{key}")
    end
    {
      directory?: false,
      file?: false,
    }
  rescue Aws::S3::Errors::ServiceError
    @logger.error($!.message)
    raise $!
  end

  def mkdir(path)
    @counter[:PUT] += 1
    @logger.info("PUT    s3://#{@bucket}/#{path.sub(/^\//, '')}/")
    @client.put_object({
      body: '',
      bucket: @bucket,
      key: "#{path.sub(/^\//, '')}/",
    })
    @cache.write("#{@bucket}/#{path.sub(/^\//, '')}", {
      directory?: true,
      file?: false,
      size: 0,
      times: FuseFS::FuseDir::INIT_TIMES,
    })
  rescue Aws::S3::Errors::ServiceError
    @logger.error($!.message)
  end

  def raw_close(path, raw = nil)
    @logger.debug("raw_close: path=#{path}, raw=#{raw}")
    if raw[:mode] == 'w' || raw[:mode] == 'rw' then
      if raw[:upload_id] then
        if raw[:buffer].size > 0 then
          raw[:threads] << upload_part_in_thread(path, raw)
          raw[:buffer] = nil # garbage collection
        end
        raw[:threads].each { |thread| thread.join }
        parts = Array.new(raw[:completed].size) { raw[:completed].pop }.sort_by { |part| part[:part_number] }
        @logger.info("COMPLETE_MULTIPART_UPLOAD s3://#{@bucket}/#{path.sub(/^\//, '')}")
        @client.complete_multipart_upload({
          bucket: @bucket,
          key: path.sub(/^\//, ''),
          upload_id: raw[:upload_id],
          multipart_upload: { parts: parts },
        })
      else
        @counter[:PUT] += 1
        @logger.info("PUT   s3://#{@bucket}/#{path.sub(/^\//, '')}")
        @client.put_object({
          body: raw[:buffer].string,
          bucket: @bucket,
          key: path.sub(/^\//, ''),
        })
        raw[:total_size] = raw[:buffer].size
      end
      @cache.write("#{@bucket}/#{path.sub(/^\//, '')}", {
        directory?: false,
        file?: true,
        size: raw[:total_size],
        times: Array.new(3, Time.now),
      })
    end
  rescue Aws::S3::Errors::ServiceError
    @logger.error($!.message)
  end

  def raw_open(path, mode, rfusefs = nil)
    @logger.debug("raw_open: path=#{path}, mode=#{mode}, rfusefs=#{rfusefs}")
    raw = {
      mode: mode,
      buffer: StringIO.new,
      threads: Array.new,
      total_size: 0,
    }
    if mode == 'r' || mode == 'rw' then
      raw[:range] = 0...0
      raw[:total_size] = getattr(path)[:size]
    end
    raw
  rescue Aws::S3::Errors::ServiceError
    @logger.error($!.message)
    {
      mode: mode,
      buffer: StringIO.new,
      threads: Array.new,
      total_size: 0,
    }
  end

  def raw_read(path, offset, size, raw = nil)
    @logger.debug("raw_read: path=#{path}, offset=#{offset}, size=#{size}, raw=#{raw}")
    return '' if raw[:total_size] == 0
    unless raw[:range].cover?(offset) && raw[:range].cover?(offset + size - 1) then
      last = [offset + @buffer_size - 1, getattr(path)[:size]].min
      @counter[:GET] += 1
      @logger.info("GET    s3://#{@bucket}/#{path.sub(/^\//, '')} [bytes=#{offset}-#{last}]")
      resp = @client.get_object({
        bucket: @bucket,
        key: path.sub(/^\//, ''),
        range: "bytes=#{offset}-#{last}",
      })
      raw[:range] = offset..(offset + resp.content_length - 1)
      raw[:buffer] = resp.body
    end
    raw[:buffer].seek(offset - raw[:range].first)
    raw[:buffer].read(size) || ''
  rescue Aws::S3::Errors::ServiceError
    @logger.error($!.message)
  end

  def raw_sync(path, datasync, raw = nil)
    @logger.debug("raw_sync: path=#{path}, datasync=#{datasync}, raw=#{raw}")
  end

  def raw_truncate(path, offset, raw = nil)
    @logger.debug("raw_truncate: path=#{path}, offset=#{offset}, raw=#{raw}")
  end

  def raw_write(path, offset, size, buffer, raw = nil)
    @logger.debug("raw_write: path=#{path}, offset=#{offset}, size=#{size}, raw=#{raw}")
    raw[:buffer].write(buffer)
    if raw[:buffer].size > MULTIPART_CHUNKSIZE then
      unless raw[:upload_id] then
        @logger.info("INITIATE_MULTIPART_UPLOAD s3://#{@bucket}/#{path.sub(/^\//, '')}")
        resp = @client.create_multipart_upload({
          bucket: @bucket,
          key: path.sub(/^\//, ''),
        })
        raw[:upload_id] = resp.upload_id
        raw[:completed] = Queue.new
        raw[:semaphore] = Queue.new
        raw[:part_number] = 0
        MAX_THREADS.times do
          raw[:semaphore] << :lock
        end
      end
      raw[:threads] << upload_part_in_thread(path, raw)
      raw[:buffer] = StringIO.new
    end
    size
  rescue Aws::S3::Errors::ServiceError
    @logger.error($!.message)
  end

  def upload_part_in_thread(path, raw = nil)
    lock = raw[:semaphore].pop
    thread_part_number = raw[:part_number] += 1
    raw[:total_size] += raw[:buffer].size
    buffer = raw[:buffer].clone
    thread = Thread.new do
      @counter[:PUT] += 1
      @logger.info("PUT    s3://#{@bucket}/#{path.sub(/^\//, '')} [part_number=#{thread_part_number}]")
      resp = @client.upload_part({
        body: buffer.string,
        bucket: @bucket,
        key: path.sub(/^\//, ''),
        part_number: thread_part_number,
        upload_id: raw[:upload_id],
      })
      buffer = nil # garbage collection
      raw[:completed] << {
        etag: resp.etag,
        part_number: thread_part_number,
      }
      raw[:semaphore] << lock
    end
    thread.abort_on_exception = true
    thread
  end

  def rename(from_path, to_path)
    if directory?(from_path) then
      objects = contents(from_path, '')
    else
      objects = [from_path.sub(/^\//, '')]
    end
    objects.each do |item|
      key = item.sub(from_path.sub(/^\//, ''), to_path.sub(/^\//, ''))
      @counter[:COPY] += 1
      @logger.info("COPY   s3://#{@bucket}/#{item} -> s3://#{@bucket}/#{key}")
      @client.copy_object({
        bucket: @bucket,
        copy_source: "/#{@bucket}/#{item}",
        key: key,
      })
      @cache.write("#{@bucket}/#{key}", getattr(item))
    end
    @counter[:DELETE] += objects.size
    @logger.info("DELETE #{objects.map { |item| "s3://#{@bucket}/#{item}" }.join(' ')}")
    @client.delete_objects({
      bucket: @bucket,
      delete: {
        objects: objects.map { |item| { key: item } },
      },
    })
    objects.each { |item| @cache.delete("#{@bucket}/#{item.sub(/\/$/, '')}") }
  rescue Aws::S3::Errors::ServiceError
    @logger.error($!.message)
  end

  def rmdir(path)
    @counter[:DELETE] += 1
    @logger.info("DELETE s3://#{@bucket}/#{path.sub(/^\//, '')}/")
    @client.delete_object({
      bucket: @bucket,
      key: "#{path.sub(/^\//, '')}/",
    })
    @cache.delete("#{@bucket}/#{path.sub(/^\//, '')}")
  rescue Aws::S3::Errors::ServiceError
    @logger.error($!.message)
  end

  def size(path)
    getattr(path)[:size]
  end

  def times(path)
    return FuseFS::FuseDir::INIT_TIMES if path == '/'
    getattr(path)[:times]
  end

  def xattr(path)
    { circus: Circus::CIRCUS_VERSION }
  end

end
