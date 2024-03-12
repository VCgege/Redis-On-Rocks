test "(start-init) Flush config and compare rewrite config file lines" {
    foreach_sentinel_id id {
        assert_match "OK" [S $id SENTINEL FLUSHCONFIG]
        set file1 ../tests/includes/sentinel.conf
        set file2 [file join "sentinel_${id}" "sentinel.conf"] 

        set fh1 [open $file1 r]
        set file_data [read $1]
        puts $file_data

        set fh2 [open $file2 r]
        set file_data [read $fh2]
        puts $file_data


        set fh1 [open $file1 r]
        set fh2 [open $file2 r]
        while {[gets $fh1 line1]} {
            puts "\nfh1: $line1"
            if {[gets $fh2 line2]} {
                puts "fh2: $line2"
            }
        }



        
        while {[gets $fh1 line1]} {
            puts "fh1: $line1"
            if {[gets $fh2 line2]} {
                puts "fh2: $line2"
                assert [string equal $line1 $line2]
            } else {
                puts "fh2: $line2"
                fail "sentinel config file rewrite sequence changed"
            }
        }
        close $fh1
        close $fh2  
    }
}