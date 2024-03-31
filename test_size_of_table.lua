local concat = table.concat
local log_buffer_index = 0

local all_buffer_arr = {}
local succ, new_tab = pcall(require, "table.new")
if not succ then
    new_tab = function () return {} end
end
local function log(msg)
    print(msg)
end
local function printTable( t )
    local printTable_cache = {}
    local function sub_printTable( t, indent )
        if ( printTable_cache[tostring(t)] ) then
            log( indent .. "*" .. tostring(t) )
        else
            printTable_cache[tostring(t)] = true
            if ( type( t ) == "table" ) then
                for pos,val in pairs( t ) do
                    if ( type(val) == "table" ) then
                        log( indent .. "[" .. pos .. "] => " .. tostring( t ).. " {" )
                        sub_printTable( val, indent .. string.rep( " ", string.len(pos)+8 ) )
                        log( indent .. string.rep( " ", string.len(pos)+6 ) .. "}" )
                    elseif ( type(val) == "string" ) then
                        log( indent .. "[" .. pos .. '] => "' .. val .. '"' )
                    else
                        log( indent .. "[" .. pos .. "] => " .. tostring(val) )
                    end
                end
            else
                log( indent..tostring(t) )
            end
        end
    end
  
    if ( type(t) == "table" ) then
        log( tostring(t) .. " {" )
        sub_printTable( t, "  " )
        log( "}" )
    else
        sub_printTable( t, "  " )
    end
end

local log_buffer_data = new_tab(20000, 0)
local function write_buf(msg)
    log_buffer_index = log_buffer_index + 1
    log_buffer_data[log_buffer_index] = msg
end

local t1 = {10, 20, 30, key1 = "value1", key2 = "value2"}
write_buf(t1)
local t2 = {20, 30, 40, key1 = "value5", key2 = "value6"}
write_buf(t2)
local t3 = {30, 40, 50, key1 = "value7", key2 = "value8"}
write_buf(t3)

for i=0,log_buffer_index do
    table.insert(all_buffer_arr,log_buffer_data[i] )
end
printTable(all_buffer_arr)
-- print(#t)  -- Output: 3 (the length of the array part)

local succ, new_tab = pcall(require, "table.new")
if not succ then
    new_tab = function () return {} end
end

-- for i=1,5.+1 do
--     local msg="Hello World" .. i
--     log_buffer_index = log_buffer_index + 1
--     log_buffer_data[log_buffer_index] = msg
-- end
-- local packet = concat(log_buffer_data, "", 1, log_buffer_index)

-- print(packet)
