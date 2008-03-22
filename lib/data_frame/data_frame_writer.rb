class DataFrame
  def write_to_tsv(filename=nil, header=true, row_names=false)
    write_to_csv(filename, header, row_names, "\t")
  end

  def write_csv(filename, header=true)
    FasterCSV.open(filename, "w") do |csv|
      if header
        csv << col_names
      end
      @data.each do |d|
        csv << d
      end
    end
  end
  
  def to_csv(header=true)
    FasterCSV.generate { |csv|
      if header
        csv << col_names
      end
      @data.each do |d|
        csv << d
      end
    end
  
  end
  
  
  def html_table
    str = "<table>" + "<tr>" + col_names.map{|n| "<th>#{n}</th>"}.join("") + "</tr>"
     
    str += @data.map { |d| row = d.map {|r| "<td>#{r.inspect}</td>" }; "<tr>#{row}</tr>" }.join
    str += "</table>"
    str
  end
end