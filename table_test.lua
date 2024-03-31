
local function printTable( t )
    local printTable_cache = {}
    local function sub_printTable( t, indent )
        if ( printTable_cache[tostring(t)] ) then
            print( indent .. "*" .. tostring(t) )
        else
            printTable_cache[tostring(t)] = true
            if ( type( t ) == "table" ) then
                for pos,val in pairs( t ) do
                    if ( type(val) == "table" ) then
                        print( indent .. "[" .. pos .. "] => " .. tostring( t ).. " {" )
                        sub_printTable( val, indent .. string.rep( " ", string.len(pos)+8 ) )
                        print( indent .. string.rep( " ", string.len(pos)+6 ) .. "}" )
                    elseif ( type(val) == "string" ) then
                        print( indent .. "[" .. pos .. '] => "' .. val .. '"' )
                    else
                        print( indent .. "[" .. pos .. "] => " .. tostring(val) )
                    end
                end
            else
                print( indent..tostring(t) )
            end
        end
    end
    if ( type(t) == "table" ) then
        print( tostring(t) .. " {" )
        sub_printTable( t, "  " )
        print( "}" )
    else
        sub_printTable( t, "  " )
    end    
end
local response_status = "404"
local response_key = "response_code"
local enforcer_descriptors_key = "descriptors"
local enforcer_descriptors = "[{\"profile\": \"test/waas-api-profile\",\"tag\": \"enforcer\",\"policy\": \"apiPolicy\",\"source\": \"222.222.223.111\",\"classifier\": \"httpbin-sample\",\"actor\": \"\",\"hash\": \"cs:true;sp:false\",\"rate\": \"5m\",\"rule_name\": \"404-status-code\",\"field\": \"response_code\",\"values\": \"404;\",\"block_period\": \"5m\"}]"

-- Build message table
local response_status_table = {}
response_status_table["key"] = response_key
response_status_table["value"] = { string_value = response_status }

local enforcer_descriptors_table = {}
enforcer_descriptors_table["key"] = enforcer_descriptors_key
enforcer_descriptors_table["value"] = { string_value = enforcer_descriptors}

local my_table = {resource_logs={{scope_logs={{log_records={{attributes={ enforcer_descriptors_table, response_status_table } } } } } } } }
printTable(my_table)


