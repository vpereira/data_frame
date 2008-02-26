class DataFrame
  
  # Creates a data frame from a csv file.
  # header=true means that the columns are named in the first line.
  # name_col chooses a column to be the name of the rows.
  # If an integer, name_col represents the offset of the column.
  # If a string, name_col represents the name of the column.
  
  def self.create_from_csv(filename, header=true, name_col=nil, fcsv_opts={})
    opts = {:headers => header, :converters => :numeric}.merge(fcsv_opts)
    data = FasterCSV.read(filename, opts)
    
    df = DataFrame.new
    
    if header
      headers = data.headers
      case name_col
      when String
        headers.delete(name_col) 
      when Fixnum
        headers.delete_at(name_col)
      when NilClass
      else
        raise ArgumentError.new("Illegal type for name_column")
      end
      headers.each { |h| df.add_col(h) }
    else
      if name_col.kind_of?(Fixnum)
        num_cols = data[0].size - 1
      elsif name_col.kind_of?(NilClass)
        num_cols = data[0].size
      else
        raise ArgumentError.new("Illegal type for name_column")
      end
      df.num_cols = num_cols

    end

    data.each do |row|
      if header
        row_name = row.delete(name_col) if !name_col.nil?
        df.add_row(row.to_hash)
      else
        row.delete_at(name_col) if !name_col.nil?
        df.add_row(row)
      end
    end
    
    df
  end

  # no escape/quoting allowed
  def self.create_from_tsv(filename, header=true, name_col=nil)

    # need 1.2.1 to change quote_char and this is a dumb hack to try to turn it off anyway
    create_from_csv(filename, header, name_col, {:quote_char=>"\0", :col_sep=>"\t"})

    ## really really lame...
    # system %{cat #{filename} | perl -pe 's/"/\\"/g; s/^/"/; s/\\t/"\\t"/g; s/$/"/' > #{filename}.quoted}
    # create_from_csv("#{filename}.quoted",header,nil,{:col_sep=>"\t"})
    

    ## our own is faster, but no data type conversion
    # sep = "\t"

    # file = open(filename)
    # if header
    #   headerline = file.readline.chomp
    #   col_names = headerline.split(sep)
    # else
    #   raise "only support with headers right now"
    # end
    # df = DataFrame.new([],[], col_names)

    # file.each do |line|
    #   line.chomp!
    #   df.add_row(line.split(sep))
    # end
    # df
    # ensure
    #   file.close
  end

  def self.create_from_mysql(mysql_connection, sql)
    raise NotImplementedError
  end
end

