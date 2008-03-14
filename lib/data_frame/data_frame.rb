class DataFrame

  class DuplicateIdError < ArgumentError
  end
  
  class IncompatibleDimensionError < ArgumentError
  end

  class BadRHSTypeError < ArgumentError
  end

  begin
    require 'highline'
    @@highline = HighLine.new
  rescue LoadError; end


  attr_reader :col_names, :row_names, :names_to_index, :data
  
  # Initializes from two dimensional data array.
  # You can optionally provide row names or column names.  
  # Missing row or column names with be turned into "_N" where N is the number of row or column.

  def initialize(data=[], row_names=[], col_names=[])
    raise ArgumentError unless data.kind_of?(Array)
    raise ArgumentError unless row_names.kind_of?(Array)
    raise ArgumentError unless col_names.kind_of?(Array)

    @data = data
    @row_names = row_names
    @col_names = col_names
    
    # handle anonymous row or column names
    if data.size > 0
      row_names[data.size-1] ||= nil 
      col_names[data[0].size-1] ||= nil 
      row_names.each_index { |r| row_names[r] ||= "_#{r}" }
      col_names.each_index { |c| col_names[c] ||= "_#{c}" }
    end

    # handle empty rows
    if data == [] and row_names == []
      data = Array.new(col_names.size, [])
    end
    
    @names_to_index = Hash.new
    reindex_names
  end
  

  
  def self.create_from_array(data, row_names=[], col_names=[])
    raise ArgumentError unless data.is_a?(Array)
    case data[0]
    when Array
      df = DataFrame.new(data, row_names, col_names)
    when Hash
      df = DataFrame.new
      data.each_index do |d|
        df.add_row(data[d], row_names[d])
      end
    else 
      # coerce vector into data frame, this could be dangerous
      df = DataFrame.new([data], row_names, col_names)
    end
    df
  end
    
  def self.create_from_hash(data)
    names_to_values = Hash.new
    row_names = Hash.new
    col_names = Hash.new
    data.each_pair do |k, v|
      raise DuplicateRowIdError.new if row_names[k]
      row_names[k] = true
      names_to_values[k] = Hash.new
      raise ArgumentError unless v.is_a?(Hash)
      v.each_pair do |k2, v2|
        col_names[k2] = true
        names_to_values[k][k2] = v2
      end
    end
      
    row_names_sorted = row_names.keys.sort
    col_names_sorted = col_names.keys.sort
    data = Array.new
    row_names_sorted.each_index do |r|
      data[r] = Array.new
      col_names_sorted.each_index do |c|
        data[r][c] = names_to_values[row_names_sorted[r]][col_names_sorted[c]]
      end
    end
    DataFrame.new(data, row_names_sorted, col_names_sorted)
  end
  
  # Creates data frame from array or arrays, or array of hashes
  # * Example 1: <tt>DataFrame.create([[10, 1], [3,10]], row_names = ["giraffe", "snake"], col_names = ["height", "length"])</tt>
  # * Example 2: <tt>DataFrame.create([{"length" => 3, "height" => 10}, {"length" => 10, "height" => 1}], row_names = ["giraffe", "snake"])</tt>
  # * Example 3: <tt>DataFrame.create({"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}})</tt>
  #
  # You can optionally provide row names or column names.  
  # Missing row or column names with be turned into "_N" where N is the number of row or column.
  
  def self.create(*data)
    data = data[0] if data.size == 1
    case data
    when Hash
      if data.values[0].is_a?(Hash)
        df = self.create_from_hash(data)
      else # single nameless row hash
        df = self.create_from_array([data])
      end
    when Array
      df = self.create_from_array(data)
    else
      raise ArgumentError
    end
    df
  end
  
  # Same as create
  # 
  # * Example 1: <tt>DataFrame[{"monkey" => {"length" => 3, "height" => 4}}]</tt>
  # * Example 2: <tt>D[[10, 3], [1, 10], [3, 10]]</tt>
  
  def self.[](*data)
    create(*data)
  end
  
  # Looks up slice as specified by row and col.
  # If row and col are atomic (String or Fixnum) then single value is returned.  Otherwise a DataFrame slice is returned.
  #
  # * Example 1: <tt>d[0, 0]</tt> (Returns atomic value)
  # * Example 2: <tt>d["snake", 0]</tt> (Returns atomic value)
  # * Example 3: <tt>d[["snake"], "height"]</tt> (Returns DataFrame)
  # * Example 4: <tt>d[/s/, "height"]</tt> (Returns DataFrame)
  # * Example 5: <tt>d[1, true]</tt> (Returns all columns)
  # * Example 6: <tt>d[[0,1], 0]</tt>
  # * Example 7: <tt>d[0..1, 1]</tt>
  # * Example 8: <tt>d[Proc.new {|v| v == "snake"}, Proc.new {|v| v != "height"}]</tt>
  
  def [](row, col=true)
    row_idxs, col_idxs = row_col_indices(row, col)
    if DataFrame.atomic?(row) and DataFrame.atomic?(col)
      if row_idxs.nil? or col_idxs.nil?
        r = nil
      else
        r = self.data[row_idxs][col_idxs]
      end
    else
      r = slice(row_idxs, col_idxs)
    end
    r
  end
  
  # Sets value in slice as specified by row and col
  # If row and col are atomic (String or Fixnum) then can be set to atomic value.
  # Otherwise value needs to be set to DataFrame with dimensions matching the selected slice.
  # 
  # See [] for examples of how to select the slice.  
  #
  # If row or col is a string and doesn't exist, a row or col will be created.
  # If row or col is an integer and is higher than the number of rows or cols, enough rows or cols will be created to make room for value.

  
  def []=(*args)
    row = args[0]  
    case args.length
    when 2
      col = true
      value = args[1]
    when 3
      col = args[1]
      value = args[2]
    else
      raise ArgumentError
    end
    
    expand_to_fit_indices(row, col)

    row_idxs, col_idxs = row_col_indices(row, col)
    set_slice(row_idxs, col_idxs, value)
  end
  
  # Adds a row with an optional name.
  # row_data can either be 
  # 1 an array the exactly size of the number of columns.
  # 2 a hash, example: <tt>d << {"bug" => {"length" => 1, "height" => 1}}</tt>
  
  def add_row(row_data, row_name=nil)
    case row_data
    when Hash
      # if the hash is a single element assume it's row_name => { data }
      if row_data.size == 1 and (row_data.values[0].is_a?(Hash) or row_data.values[0].is_a?(Array))
        add_row(row_data.values[0], row_data.keys[0])
      else
        add_row_hash(row_data, row_name)
      end
    when Array
      add_row_array(row_data, row_name)
    end

    nil
  end
  
  # Adds a column.  Also see merge_by_row.

  def add_col(column_name=nil)
    column_name = "_#{@col_names.length}" if column_name.nil?
    raise DuplicateIdError.new(column_name) if @names_to_index[:col][column_name]
    @col_names << column_name
    @names_to_index[:col][column_name] = @col_names.size - 1
    @data.each { |d| d << nil if d.size > 0} 
  end
  
  # Adds a data frame to the bottom of the current data frame.  Columns will be
  # aligned or created as necessary.
  
  def append_data_frame(df)
    df.named_rows.each do |r|
      add_row(r)
    end
  end
  
  def prefix_col_names(prefix)
    raise NotImplementedError.new
  end

  # Adds a prefix string to all column names.  Useful for when you want to merge results of a slice back into the original data frame.

  def prefix_col_names!(prefix)
    @col_names.map! { |name| prefix + name }
    @col_names.each_index { |c| @names_to_index[:col][@col_names[c]] = c}
  end
  
  # Append the columns of data frame d into the right of frame, matching by row name.
  
  def merge_by_row(d)
    raise NotImplementedError if self.row_names != d.row_names
    col_name_overlap = (self.col_names + d.col_names).nonuniq
    raise DuplicateIdError.new(col_name_overlap.to_s) unless col_name_overlap.size == 0
    self.data.each_index do |i|
      self.data[i] += d.data[i]
    end
    d.col_names.each_index do |i|
      self.col_names << d.col_names[i]
      @names_to_index[:col][d.col_names[i]] = @col_names.size - 1
    end
  end
  
  # Append to the bottom of self, adding columns as necessary.
  # d can be a Hash, Array, or DataFrame and will be handled accordingly.
  
  def <<(d)
    case d
    when DataFrame
      append_data_frame(d)
    when Array
      add_row(d)
    when Hash
      add_row(d)
    else
      raise ArgumentError.new("Appending illegal value #{d}")
    end
  end


  
  def set_slice(row_idxs, col_idxs, value)
    row_idxs = fix_slice_input(row_idxs)
    col_idxs = fix_slice_input(col_idxs)
    
    return IncompatibleDimensionError.new("Slice has 0 rows") if row_idxs == nil or row_idxs == []
    return IncompatibleDimensionError.new("Slice has 0 cols") if col_idxs == nil or col_idxs == []  

    case value
    when DataFrame
      if (!row_idxs.size == value.data.size and col_idxs.size == value.data[0].size)
        raise IncompatibleDimensionError.new("Slice is #{row_idxs.size} x #{col_idxs.size}, data is #{value.data.size} x #{value.data[0].size}")
      end
      row_idxs.each_index do |ri|
        col_idxs.each_index do |ci|
          @data[row_idxs[ri]][col_idxs[ci]] = value.data[ri][ci]
        end
      end   
    else
      row_idxs.each do |r|
        col_idxs.each do |c|
          @data[r][c] = value
        end
      end
    end
  end
  
  def slice(row_idxs, col_idxs)
    row_idxs = fix_slice_input(row_idxs)
    col_idxs = fix_slice_input(col_idxs)
    
    return nil if row_idxs == nil or row_idxs == []
    return nil if col_idxs == nil or col_idxs == []  
    
    row_names = row_idxs.map { |r| @row_names[r] }
    col_names = col_idxs.map { |r| @col_names[r] }
    
    data = Array.new
    row_idxs.each_index do |r|
      data[r] = Array.new
      col_idxs.each_index do |c|
        data[r][c] = @data[row_idxs[r]][col_idxs[c]]
      end
    end

    d = DataFrame.new(data, row_names, col_names)
  end
  
  # Convert DataFrame to array.  If array has only one column, will be
  # turned into 1d array instead of 2d array with single elements.  Likewise
  # for a singleton row.
  
  def to_array(force_preserve_dims=false)
    if force_preserve_dims
      @data
    elsif col_names.size == 1 and row_names.size == 1
      @data[0][0]
    elsif row_names.size == 1
      @data[0]
    elsif col_names.size == 1
      @data.map { |d| d[0] }
    else
      @data
    end
  end
  
  # Return the rows as array of hashes.  Row names are attached as .row_name
  
  def cols
    cols = Hash.new
    @data.each_index do |r|
      @data[r].each_index do |c|
        name = @col_names[c]
        cols[name] ||= Array.new
        cols[name] << @data[r][c]
      end
    end
    cols
  end
  
  # Return the rows as array of hashes.  Row names are lost.
  
  def rows
    rows = Array.new
    @data.each_index do |r|
      row = Hash.new
      @data[r].each_index do |c|
        name = @col_names[c]
        row[name] = @data[r][c]
      end
      rn = @row_names[r]
      row.instance_eval do
        @row_name = rn
        def self.row_name
          @row_name
        end
      end
      rows << row
    end
    rows
  end
  
  # Return the rows as array of a single hash of hashes.  
  # A row looks like: <tt>{row_name => {col1_name => col1_val, ... ,colN_name => colN_val}}</tt>
  
  def named_rows
    named_rows = Array.new
    row_array = rows
    row_array.each_index do |r|
      named_row = Hash.new
      named_row[row_names[r]] = row_array[r]
      named_rows << named_row
    end
    named_rows
  end
  
  def by
  end
  
  # Each element of data.
  
  def each
    @data.each { |r| r.each {|e| yield e } }
  end
  
  # Each index as row_id, col_id pair. 
  
  def each_index
    indices(true, true).each { |i| yield i }
  end
  
  # Each index as row_name, col_name pair.
  
  def each_named_index
    indices(true, true).each { |b| yield [@row_names[b[0]], @col_names[b[1]]] }
  end
  
  def map!
    @data.map! { |r| r.map! {|e| yield e }}
  end
  
  # Like Enumerable map but returns a DataFrame with same dimension, row_names
  # and col_names
  
  def map
    D[@data.map { |r| r.map {|e| yield e }}]
  end
  
  def indices(row, col)
    ridxs, cidxs = row_col_indices(row, col)
    a = Array.new
    ridxs.each do |r|
      cidxs.each do |c|
        a << [r,c]
      end
    end
    a
  end
  
  def row_col_indices(row, col) 
    [row_indices(row), col_indices(col)]
  end
  
  def row_indices(selector = true) 
    axis_indices(selector, :row)
  end
  
  def col_indices(selector = true) 
    axis_indices(selector, :col)
  end

  def get_value(row_idx, col_idx)
    @data[row_idx][col_idx]
  end
  
  def set_value(value, row_idx, col_idx)
    @data[row_idx][col_idx] = value
  end

  # Compare with other DataFrame or else return a DataFrame of elementwise comparisons
  def ==(other)
    case other
    when DataFrame
      @data == other.data
    when Enumerable
      to_array(true) == other
    else
      map { |x| x == other }
    end
    # else
    #   raise ArgumentError, "DataFrame has no equality test against class #{other.class}"
    # end
  end
  
  def ===(other)
    self == other
  end

  # like Enumerable#all?
  def all?(&block); call_on_elements(:all?, &block); end
  # like Enumerable#any?
  def any?(&block); call_on_elements(:any?, &block); end

  # Intended for Enumerable-like methods that return a single value
  # (ones that return data structures need to be custom for DataFrame, e.g. map())
  def call_on_elements(method, &block)
    Enumerable::Enumerator.new(self).send(method, &block)
  end

  def polymorphic_elementwise_binary_call(other, &block)
    # if other.is_a? DataFrame
    case other
    when DataFrame
      raise "Need to have same sized DataFrames" unless self.size == other.size
      raise NotImplementedError
    when Enumerable
      raise BadRHSTypeError
    else
      # maybe use/extend DataFrame.atomic?
      # elsif [Numeric,String,TrueClass,FalseClass].any?{|c| other.is_a?(c)}
      map { |x| block.call(x, other) }
    end
    # else
    #   raise BadRHSTypeError
    # end
  end

  def polymorphic_elementwise_binary_op(op, other)
    begin
      polymorphic_elementwise_binary_call(other) do |self_elt, other_elt|
        self_elt.send(op, other_elt)
      end
    rescue BadRHSTypeError
      raise BadRHSTypeErrorError, "For binary operator #{op.inspect} bad RHS: #{other.inspect} -- #{$!.inspect}"
    end
  end

  # elementwise addition
  def +(o)  polymorphic_elementwise_binary_op(:+, o)  end
  # elementwise subtraction
  def -(o)  polymorphic_elementwise_binary_op(:-, o)  end
  # elementwise multiplication
  def *(o)  polymorphic_elementwise_binary_op(:*, o)  end
  # def /(o)  polymorphic_elementwise_binary_op(:/, o)  end
  
  # Bitwise ops overridden for logical elementwise boolean ops
  # (for comparison, Enumerables have set semantics on these)
  # (also, note we are not allowed to override &&, ||, !, in ruby)

  # elementwise logical and
  def &(o)  polymorphic_elementwise_binary_call(o) {|x,o2| x && o2} end
  # elementwise logical or
  def |(o)  polymorphic_elementwise_binary_call(o) {|x,o2| x || o2} end
  # elementwise logical not
  def not;  map{|x| !x}; end

  # elementwise =~ and coerce to boolean (for convenience)
  def =~(other)
    polymorphic_elementwise_binary_call(other) { |x,o| (x =~ o) ? true : false }
  end

  def to_f;  map {|x| x.to_f};  end
  def to_i;  map {|x| x.to_i};  end


  # do we need this?  why not...
  def transpose
    new_data = (0...num_cols).map{ Array.new(num_rows) }
    each_index do |i,j|
      new_data[j][i] = @data[i][j]
    end
    DataFrame.new(new_data, col_names, row_names)
  end


  alias t transpose

  # d/col is a shortcut for <tt>d[true, col]</tt>
  
  def /(col)
    self[true, col]
  end

  # d.my_col_name is a shortcut for <tt>d[true, 'my_col_name']</tt>
  def method_missing(name, *args)
    name_s = name.to_s
    if name_s[-1..-1] == "="
      raise NotImplementedError, "not tested yet"
      self[true,name_s[0..-2]] = args[0]
    end
    if col_names.member?(name_s)
      self[true, name_s]
    else
      raise NameError, "No DataFrame method with name: #{name}"
    end
  end
  
  def sort_rows_by_col(col_name, ascending=true)
    raise "Column not defined" if names_to_index[:col][col_name].nil?
    if ascending
      sort_rows{ |b, a| b[true,col_name].v <=> a[true,col_name].v }
    else
      sort_rows{ |a, b| b[true,col_name].v <=> a[true,col_name].v }
    end
  end
  
  def sort_cols_by_row(row_name, ascending=true)
    raise "Row not defined" if names_to_index[:row][row_name].nil?
    if ascending
      sort_cols{ |b, a| b[row_name,true].v <=> a[row_name,true].v }
    else  
      sort_cols{ |a, b| b[row_name,true].v <=> a[row_name,true].v }
    end
  end

  def sort_rows
    new_col_names = self.col_names
    new_row_names = self.row_names.sort { |r1, r2| yield self[r1, true], self[r2, true] }

    new_names_to_index = Hash.new
    new_names_to_index[:row] = Hash.new
    new_data = Array.new
    new_row_names.each_index { |r| new_names_to_index[:row][new_row_names[r]] = r}
    
    @row_names.each_index do |r|
      new_index = new_names_to_index[:row][row_names[r]] 
      new_data[new_index] = self.data[r]
    end
    
    DataFrame.new(new_data, new_row_names, new_col_names)
  end
  
  def sort_cols
    new_row_names = self.row_names
    new_col_names = self.col_names.sort { |c1, c2| yield self[true, c1], self[true, c2] }

    new_names_to_index = Hash.new
    new_names_to_index[:col] = Hash.new
    new_data = Array.new
    new_col_names.each_index { |c| new_names_to_index[:col][new_col_names[c]] = c}
    
    @row_names.each_index { |r| new_data[r] = Array.new }
    
    @col_names.each_index do |c|
      new_index = new_names_to_index[:col][col_names[c]] 
      new_data.each_index { |r| new_data[r][new_index] = @data[r][c] }
    end
    
    DataFrame.new(new_data, new_row_names, new_col_names)
  end
  
  def by(col_name)
    return if num_rows == 0
    puts col_name
    
    sort_d = sort_rows_by_col(col_name)
    
    start_rows = Array.new
    key = sort_d/col_name
    last_val = key[0].v
    last_index = 0
    index = 0
    key.each do |val|
      if val != last_val
        yield sort_d[last_index...index, true]
        last_index = index
        last_val = val
      end
      index += 1
    end

    yield sort_d[last_index...index, true]
  end
  
  # Sorts rows and cols by string order
  
  def resort!
    resort_rows!
    resort_cols!
  end
  

  
  def resort_rows!
    return if @row_names.sort == @row_names # for efficiency

    new_row_names = row_names.sort
    new_names_to_index = Hash.new
    new_names_to_index[:row] = Hash.new
    new_row_names.each_index { |r| new_names_to_index[:row][new_row_names[r]] = r}

    new_data = Array.new

    @row_names.each_index do |r|
      new_index = new_names_to_index[:row][row_names[r]] 
      new_data[new_index] = @data[r]
    end
    
    @data = new_data
    @row_names = new_row_names
    @names_to_index[:row] = new_names_to_index[:row]
  end
  
  def resort_cols!
    return if @col_names.sort == @col_names # for efficiency

    new_col_names = col_names.sort
    new_names_to_index = Hash.new
    new_names_to_index[:col] = Hash.new
    new_col_names.each_index { |c| new_names_to_index[:col][new_col_names[c]] = c}

    new_data = Array.new
    @row_names.each_index { |r| new_data[r] = Array.new }

    @col_names.each_index do |c|
      new_index = new_names_to_index[:col][col_names[c]] 
      new_data.each_index { |r| new_data[r][new_index] = @data[r][c] }
    end
    
    @data = new_data
    @col_names = new_col_names
    @names_to_index[:col] = new_names_to_index[:col]
  end
  
  def reindex_cols
    if cols
      @names_to_index[:col] = Hash.new
      @col_names.each_index { |c| @names_to_index[:col][@col_names[c]] = c}
      if @data.size > 0 and @col_names.size != @data[0].size
        throw NotImplementedError.new("Col names size: #{@col_names.size} different than number of data rows #{@data[0].size}")
      end
    end
  end
  
  def reindex_rows
    if rows
      @names_to_index[:row] = Hash.new
      @row_names.each_index { |r| @names_to_index[:row][@row_names[r]] = r}
      if (@row_names.size != @data.size)
        throw NotImplementedError.new("Row names size: #{@row_names.size} different than number of data rows #{@data.size}")
      end
    end
  end

  def reindex_names
    reindex_rows
    reindex_cols
  end

  # Expands the number of rows to num_r

  def num_rows=(num_r)
    (num_r - num_rows).times do
      add_row_array(Array.new(num_cols, nil), nil)
    end
  end
  
  # Expands the number of cols to num_c
  
  def num_cols=(num_c)
    (num_c - num_cols).times do
      add_col
    end
  end
  
  def num_rows
    self.data.size
  end
  
  def num_cols
    self.col_names.size
  end

  def size
    [num_rows, num_cols]
  end

  alias v to_array
  alias cbind merge_by_row

  def inspect(type=:default)
    if type == :default
      type = (num_rows < 100) ? :table : :tabs
    end

    s = ""
    s += "# "  if type == :records   # for legit yaml
    s << "DataFrame size=(#{num_rows},#{num_cols}):\n"        # newline here convenient for irb
    cn = col_names

    if type == :table
      # TODO separate out columns like R ?
      max_sizes = cn.map do |n|
        [n.size, 
         self[true,n] ? self[true,n].map{|x| x.inspect.size}.v(true).flatten : nil
        ].flatten.max
      end
      use_col_names = @col_names.any?{|n| n !~ /^_\d+$/}
      use_row_names = @row_names.any?{|n| n !~ /^_\d+$/}

      max_row_name_size = @row_names.map{|n| n.size}.max

      if use_col_names
        s <<  " " * (max_row_name_size+1) if use_row_names
        cn.each_with_index do |n,i|
          s << color("%-#{max_sizes[i]+1}s" % [n], :blue)
        end
        s << "\n"
      end
      rows.each_with_index do |row,i|
        if use_row_names
          s << color("%-#{max_row_name_size+1}s" % [@row_names[i]], :blue)
        end
        cn.each_with_index do |n,j|
          s << ("%-#{max_sizes[j] + 1}s" % [row[n].inspect])
        end
        s << "\n"
      end
    elsif type == :tabs
      s << color(cn.join("\t"), :blue) << "\n"
      rows.each do |r|
        s << cn.map{|n| r[n].inspect}.join("\t") << "\n"
      end
    elsif type == :records
      s << to_yaml_rows
    else
      raise ArgumentError, "Doesn't support printing type #{type.inspect}"
    end

    s
  end

  def to_yaml_rows
    # one record (hash) per row
    # this is the yaml stream of documents format
    s = ""
    rows.each do |row|
      # should do something clever to fix key order, though probably to_yaml doesnt support, boo.
      s << row.to_yaml
    end
    s
  end
  
  private
  def color(s, *args)
    if @@highline
      @@highline.color(s, *args)
    else
      s
    end
  end

  def fix_slice_input(idxs)
    case idxs
    when Fixnum
      return [idxs]
    when Array
      return nil if idxs.size == 0
      raise ArgumentError unless idxs[0].is_a?(Fixnum)
      return idxs
    when NilClass
      return nil
    else
      raise ArgumentError
    end
  end  


  
  def axis_indices(selector, axis)
    raise ArgumentError unless axis == :row or axis == :col
    case selector
    when Fixnum
      idxs = selector
    when String
      idxs = @names_to_index[axis][selector]
    when DataFrame
      # should assert that selector has singleton dimension
      other_v = selector.v
      return axis_indices(other_v, axis)
    when Array
      # TODO test
      axis_size = (axis==:row ? num_rows : num_cols)
      if axis_size > 0 && selector.size == axis_size && 
          (selector[0].is_a?(TrueClass) || selector[0].is_a?(FalseClass))
        bools = selector
        selector = []
        bools.each_with_index { |b,i| selector << i  if b }
      end
      idxs = selector.map { |a| axis_indices(a, axis) }
    when TrueClass
      idxs = (0..@names_to_index[axis].length-1).to_a
    when FalseClass
      idxs = nil
    when Range
      idxs = selector.to_a
    when Proc
      names = @names_to_index[axis].keys.select { |e| selector.call(e) }
      idxs = names.map {|name| @names_to_index[axis][name] }
    when Regexp
      names = @names_to_index[axis].keys.select { |e| e[selector] }
      idxs = names.map {|name| @names_to_index[axis][name] }
    else
      raise ArgumentError.new("Bad argument #{selector}")
    end
    idxs
  end
  
  def add_row_hash(row_data, row_name)
    row_name = "_#{@row_names.length}" if row_name.nil? or row_name =~ /^_/
    raise DuplicateIdError.new("Row named #{row_name} repeated") if @names_to_index[:row][row_name]
    
    row_array = Array.new
    new_cols = Array.new
    row_data.each_key do |k|
      new_cols << k if @names_to_index[:col][k].nil?
    end
    
    new_cols.sort.each { |c| add_col(c) }
      
    row_data.each_pair do |k, v|
      row_array[@names_to_index[:col][k]] = v      
    end
    
    row_array[col_names.length-1] = nil if row_array[col_names.length-1].nil?
    add_row_array(row_array, row_name)
    nil
  end
  
  def add_row_array(row_data, row_name)
    row_name = "_#{@row_names.length}" if row_name.nil?
    raise DuplicateIdError if @names_to_index[:row][row_name]
    raise IncompatibleDimensionError.new("Adding row length #{row_data.length} to frame with #{col_names.length} cols") unless row_data.length == col_names.length
    @data.push(row_data)
    @row_names.push(row_name)
    @names_to_index[:row][row_name] = @row_names.size - 1
    nil
  end  
  
  def expand_to_fit_indices(row_selector, col_selector)
    expand_rows_to_fit_indices(row_selector)
    expand_cols_to_fit_indices(col_selector)
  end
  
  def expand_cols_to_fit_indices(col_selector)

    case col_selector
    when Fixnum
      raise if col_selector < 0
      self.num_cols = col_selector+1 if col_selector >=  num_cols
    when String
      add_col(col_selector) if names_to_index[:col][col_selector].nil?
    else
    end  
  end
  
  def expand_rows_to_fit_indices(row_selector)
    case row_selector
    when Fixnum

      raise if row_selector < 0
      self.num_rows = row_selector+1 if row_selector >= num_rows
    when String
      add_row(Array.new(num_cols, nil)) if names_to_index[:row][row_selector].nil?
    else
    end
  end
  
  def self.atomic?(index)
    index.is_a?(Fixnum) or index.is_a?(String)
  end
end

D = DataFrame
