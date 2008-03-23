class DataFrame
  def write_to_tsv(filename=nil, header=true, row_names=false)
    write_to_csv(filename, header, row_names, "\t", " ")
  end
  
  def write_to_csv(filename=nil, header=true, row_names=false, delim=",", delim_sub=nil)
    delim_sub ||= delim
    raise "not implemented" if row_names
    
    if filename
      f = File.open(filename, "w")
    else
      f = $stdout
    end
    if header
      f.puts col_names.join(delim)
    end
    @data.each do |row|
      f.puts row.map{|x| x.to_s.gsub(delim, delim_sub)}.join(delim)
    end
    f.close if f != $stdout
  end
  
  def write_to_csv2(filename, header=true)
    FasterCSV.open(filename, "w") do |csv|
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