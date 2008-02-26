require File.dirname(__FILE__) + '/test_helper.rb'

class TestDataFrame < Test::Unit::TestCase
  DATA = File.dirname(__FILE__) + '/data/test_data.csv'
  def setup
  end  
  def teardown
  end

  def test_generate_csv
    d = DataFrame.create_from_csv(DATA, header=true, name_col=0)
    assert(d == D[[1, 10], [10, 3]])
    d = DataFrame.create_from_csv(DATA, header=false, name_col=0)

    assert(d == D[["height", "length"], [1, 10], [10, 3]])
  end
  
  def test_generate_csv_row_names
    d = DataFrame.create_from_csv(DATA, header=true, name_col=nil)

    assert(d == D[["snake", 1, 10],["giraffe", 10, 3]])
    
    d = DataFrame.create_from_csv(DATA, header=true, name_col=1)
    assert(d == D[["snake", 10], ["giraffe", 3]]) 

    d = DataFrame.create_from_csv(DATA, header=true, name_col="length")
    assert(d == D[["snake", 1], ["giraffe", 10]])
    
    assert_raise(ArgumentError) { d = DataFrame.create_from_csv(DATA, header=false, name_col="length")}
  end
end
