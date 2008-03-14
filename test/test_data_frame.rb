require File.dirname(__FILE__) + '/test_helper.rb'

class TestDataFrame < Test::Unit::TestCase
  def setup
  end  
  def teardown
  end
  
  def test_data_frame_hash_creation
    d = DataFrame.create_from_hash({"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}})
    assert_equal(d.col_names, ["height", "length"])
    assert_equal(d.row_names, ["giraffe", "snake"])
    assert_equal(d["snake", "length"], 10)
    assert_equal(d["giraffe", "height"], 10)
    assert(d["monkey", "height"].nil?)
  end

  def test_data_frame_array_creation
    d = DataFrame.create_from_array([[10, 1], [3,10]], row_names = ["giraffe", "snake"], col_names = ["height", "length"])
    assert_equal(d.col_names, ["height", "length"])
    assert_equal(d.row_names, ["giraffe", "snake"])
    assert_equal(d["snake", "length"], 10)
    assert_equal(d["giraffe", "height"], 10)
    assert(d["monkey", "height"].nil?)
  end
  
  def test_data_frame_array_hash_creation
    d = DataFrame.create_from_array([{"length" => 3, "height" => 10}, {"length" => 10, "height" => 1}], row_names = ["giraffe", "snake"])
    assert_equal(d.col_names, ["height", "length"])
    assert_equal(d.row_names, ["giraffe", "snake"])
    assert_equal(d["snake", "length"], 10)
    assert_equal(d["giraffe", "height"], 10)
    assert(d["monkey", "height"].nil?)
  end
  
  def test_single_row_hash_creation
    d = DataFrame[{"monkey" => {"length" => 3, "height" => 4}}]
    assert_equal(3, d["monkey", "length"])
    assert_equal(1, d.row_names.length)
  end
  
  def test_single_anonymous_row_creation
    d = DataFrame[{"length" => 3, "height" => 4}]
    assert(d == D[[4,3]])
    assert(d == D[4,3]) #hmm do we want this?
  end
  
  def test_array_equality
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    assert(d == D[[10, 3], [1, 10]])
    assert(d != D[[11, 3], [1, 10]])
    assert(d != D[[10, 3], [1, 10], [3, 10]])
  end
  
  def test_hash_equality
    d = DataFrame.create_from_array([[10, 3], [1, 10]], row_names = ["giraffe", "snake"], col_names = ["height", "length"])
    assert(d == D[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}])
  end
  
  def test_singleton_equality
    d = DataFrame[{"snake" => {"length" => 10} }]
    assert(d == D[{"snake" => {"length" => 10}}] )
    assert(d == D[[10]])
    assert(d.v == 10)
  end
  
  def test_string_lookups
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    assert_equal(1, d["snake", "height"])
  end

  def test_numerical_lookups
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    assert_equal(d[0, 0], 10)
    assert_equal(d[0, 1], 3)
    assert_equal(d[1, 0], 1)
    assert_equal(d[1, 1], 10)
  end
  
  def test_basic_partial_lookups
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    assert(d["snake",true] == D[{"length" => 10, "height" => 1}])
    assert(d["snake",true] == D[1, 10])
    assert(d["snake"] == D[1, 10])
    assert(d[true, "height"] == D[[10], [1]])
    assert((d/"height") == D[[10], [1]])
  end
  
  def test_array_lookups
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    assert(d[["snake", "giraffe"], ["height"]] == D[[1], [10]])
    assert(d[["giraffe", "snake"], ["height"]] == D[[10], [1]])
    assert(d[[0,1], 0] == D[[10], [1]])
    assert(d[[1,0], 0] == D[[1], [10]])
  end
  
  def test_range_lookups
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}, "bug" => {"length" => 1, "height" => 0}}]
    assert(d[1..2, 0..1] == D[[10,3],[1,10]])
  end

  def test_regex_lookups
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    assert(d[/nak/, /.*/] == D[10, 1])
    assert(d[/CANTFIND/, true].nil?)
  end

  def test_proc_lookups
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d[Proc.new {|v| v == "snake"}, Proc.new {|v| v != "height"}] = 1
  end
  
  def test_append_row
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d << {"bug" => {"length" => 1, "height" => 1}}

    assert_raise(DataFrame::DuplicateIdError) { d << DataFrame[{"bug" => {"length" => 1, "height" => 1}}] }
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d << [1,1]
    assert(d[2] == D[1,1])
    assert(d["_2"] == D[1,1])
  end
  
  def test_resort_rows
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d << {"bug" => {"length" => 1, "height" => 1}}

    d.resort_rows!
    assert(d[2] == D[1, 10])
  end
  
  def test_resort
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d << {"bug" => {"length" => 1, "age" => 0}}

    assert(d[2] == D[nil, 1, 0])
    d.resort!
    assert(d[2] == D[nil, 1, 10])
  end
  
  def test_append_uneven_row
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d << DataFrame[{"bug" => {"length" => 1}}]
    assert(d["bug"] == D[nil,1])
    
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d << DataFrame[{"bug" => {"length" => 1, "size" => 4}}]
    assert(d["bug"] == D[nil,1,4])
    assert(d["bug", "size"] == 4)
    assert(d["snake"] == D[1,10,nil])
    
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    assert_raise(DataFrame::IncompatibleDimensionError) { d << [1,1,1] }
  end
  
  def test_append_frame
    d1 = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d2 = DataFrame[{"car" => {"length" => 9, "height" => 5}, "truck" => {"length" => 10, "height" => 6}}]
    d1 << d2
    assert(d1 == D[[10,3],[1,10],[5,9],[6,10]])
  end
  
  def test_merge_by_row
    d1 = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d2 = DataFrame[{"snake" => {"length2" => 11, "height2" => 2}, "giraffe" => {"length2" => 4, "height2" => 11}}]
    d1.merge_by_row(d2)
    assert(d1 == D[[10,3,11,4],[1,10,2,11]])
    
    assert_raise(DataFrame::DuplicateIdError) { d1.merge_by_row(d2) }
    
  end
  
  def test_set_atomic
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    assert(d == D[[10, 3], [1, 10]])
    d["giraffe", "length"] = 2

    assert(d == D[[10, 2], [1, 10]])
    
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d[0,0] = 2
    assert(d == D[[2, 3], [1, 10]])

    d[2,2] = 6
    assert(d == D[[2, 3, nil], [1, 10, nil], [nil, nil, 6]])
    
    assert_raise(RuntimeError) { d[-1,2] = 0} 
    assert_raise(RuntimeError) { d[2,-1] = 0}
    
    d[2,5] = 10
    assert(d == D[[2, 3, nil, nil, nil, nil], [1, 10, nil, nil, nil, nil], [nil, nil, 6, nil, nil, 10]])
  end
  
  
  def test_set_vector
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d[0] = D[2,2]
    assert(d == D[[2, 2], [1, 10]])
    d["giraffe"] = D[3,3]
    assert(d == D[[3, 3], [1, 10]])
    d[true,1] = D[[4],[4]]
    assert(d == D[[3, 4], [1, 4]])
    d[true,"height"] = D[[5],[5]]
    assert(d == D[[5, 4], [5, 4]])
    d[true,"age"] = D[[4], [3]]
    assert(d == D[[5, 4, 4], [5, 4, 3]])
  end
  
  def test_set_matrix
    d1 = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d2 = DataFrame[{"car" => {"length" => 9, "height" => 5}, "truck" => {"length" => 10, "height" => 6}}]
    d1 << d2
    assert(d1 == D[[10,3],[1,10],[5,9],[6,10]])
    d1[["snake", "car"], true] = D[[5,6], [7,8]]
    assert(d1 == D[[10,3],[5,6],[7,8],[6,10]])
    d1[["car", "snake"], true] = D[[5,6], [7,8]]
    assert(d1 == D[[10,3],[7,8],[5,6],[6,10]])
    d1[["car", "snake"], ["length","height"]] = D[[5,6], [7,8]]
    assert(d1 == D[[10,3],[8,7],[6,5],[6,10]])
  end
  
  def test_each
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    a = Array.new
    d.each { |e| a << e }
    assert_equal([10,3,1,10], a)
  end
  
  def test_each_index
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    a = Array.new
    d.each_index { |e| a << e }
    assert_equal(a, [[0,0], [0,1], [1,0], [1,1]])
  end
  
  def test_each_named_index
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    a = Array.new
    d.each_named_index { |e| a << e }
    assert_equal(a, [["giraffe","height"], ["giraffe","length"], ["snake","height"], ["snake","length"]])
  end
  
  def test_map
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d.map! { |e| e + 1 }
    assert(d == D[[11,4],[2,11]])
    d2 = d.map { |e| e + 1 }
    assert(d2 == D[[12,5],[3,12]])
  end
  
  def test_prefix_col_names
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    d.prefix_col_names!("S.")
    assert(d.col_names == ["S.height", "S.length"])
  end
  
  def test_sort
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}, "bug" => {"length" => 1, "height" => 0}}]
    row_sorted_d = d.sort_rows{ |a, b| b[true,"length"].v <=> a[true,"length"].v }
    assert_equal(["snake", "giraffe", "bug"], row_sorted_d.row_names)
    assert_equal(D[[1,10],[10,3],[0,1]], row_sorted_d)
    
    col_sorted_d = row_sorted_d.sort_cols{ |a, b| b["snake",true].v <=> a["snake",true].v }
    assert_equal(["length", "height"], col_sorted_d.col_names)
    assert_equal(D[[10,1],[3,10],[1,0]], col_sorted_d)

    row_sorted_d = d.sort_rows_by_col("length", ascending=false)
    assert_equal(D[[1,10],[10,3],[0,1]], row_sorted_d)

    col_sorted_d = row_sorted_d.sort_cols_by_row("snake", ascending=false)
    assert_equal(D[[10,1],[3,10],[1,0]], col_sorted_d)
  end
  
  def test_by_op
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "snake2" => {"length" => 11, "height" => 1}, "bug" => {"length" => 1, "height" => 0}}]
    a = Array.new
    d.by("height") { |df| a << df }
    
    assert_equal(D[[0,1]], a[0])
    assert_equal(D[[1,10],[1,11]], a[1])
  end
  
  def test_rows
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    assert_equal([{"giraffe" => {"length" => 3, "height" => 10}}, {"snake" => {"length" => 10, "height" => 1}}], d.named_rows)
  end
  
  def test_cols
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    assert_equal({"length" => [3, 10], "height" => [10, 1]}, d.cols)
  end
  
  def test_reindex
    d = DataFrame[{"snake" => {"length" => 10, "height" => 1}, "giraffe" => {"length" => 3, "height" => 10}}]
    assert(d == D[[10, 3], [1, 10]])
    d.col_names.map! { |name| "animal_#{name}"}
    d.reindex_names

    assert_equal(10, d["snake", "animal_length"])
    assert(d["snake", "length"].nil?)
  end

end
