local pb = require("pb")
local protoc = require("protoc")
local bit = require("bit")
local json = require("cjson")

local json
if not os.getenv("LUAUNIT") then
  json = require("cjson")
end
local request = require "http.request"

local package = "opentelemetry.proto.collector.logs.v1"
local service_request = "ExportLogsServiceRequest"
local method = "Export"
local service = "LogsService"
local proto_file = "opentelemetry/proto/collector/logs/v1/logs_service.proto"
local tcp_port = "31017"
local host = "127.0.0.1"

local response_status = "404"
local response_key = "response_code"
local enforcer_descriptors_key = "descriptors"
local enforcer_descriptors1 = "[{\"profile\": \"test/waas-api-profile\",\"tag\": \"enforcer\",\"policy\": \"apiPolicy\",\"source\": \"111.111.111.111\",\"classifier\": \"httpbin-sample\",\"actor\": \"\",\"hash\": \"cs:true;sp:false\",\"rate\": \"5m\",\"rule_name\": \"404-status-code\",\"field\": \"response_code\",\"values\": \"404;\",\"block_period\": \"5m\"}]"
local enforcer_descriptors2 = "[{\"profile\": \"test/waas-api-profile\",\"tag\": \"enforcer\",\"policy\": \"apiPolicy\",\"source\": \"222.222.222.222\",\"classifier\": \"httpbin-sample\",\"actor\": \"\",\"hash\": \"cs:true;sp:false\",\"rate\": \"5m\",\"rule_name\": \"404-status-code\",\"field\": \"response_code\",\"values\": \"404;\",\"block_period\": \"5m\"}]"

-- local tcp_port = "31017"
-- local host = "127.0.0.1"
-- local pb_file = "/usr/local/openresty/site/lualib/lua-kwaf-buffered/otel.pb"

local otel_endpoint = {}
otel_endpoint["host"] = host
otel_endpoint["port"] = tcp_port
otel_endpoint["service"] = service
otel_endpoint["method"] = method
otel_endpoint["service_request"] = service_request
otel_endpoint["package"] = package
otel_endpoint["path"] = otel_endpoint["package"] .. "." ..otel_endpoint["service"] .. "/" .. otel_endpoint["method"]


local function find_method(protos, my_package, my_service, my_method)
    for k, loaded in pairs(protos) do
      if type(loaded) == "boolean" then
        print(k)
      end
      local package = loaded.package
      for _, s in ipairs(loaded.service or {}) do
        if ("%s.%s"):format(package, s.name) ==  ("%s.%s"):format(my_package, my_service) then
          for _, m in ipairs(s.method) do
            if m.name == my_method then
              return m
            end
          end
        end
      end
    end
    return nil
end
local function file_exists(file)
    local fp = io.open(file, "r")
    if fp then
      fp:close()
      return true
    end
    return false
end
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
protoc.reload()
-- Build message table
local response_status_table = {}
response_status_table["key"] = response_key
response_status_table["value"] = { string_value = response_status }

local enforcer_descriptors_table = {}
enforcer_descriptors_table["key"] = enforcer_descriptors_key
enforcer_descriptors_table["value"] = { string_value = enforcer_descriptors1}

local enforcer_descriptors_table2 = {}
enforcer_descriptors_table2["key"] = enforcer_descriptors_key
enforcer_descriptors_table2["value"] = { string_value = enforcer_descriptors2}

local send_buffer = {}
local log_buffer_data = {}
local log_buffer_index = 0
for i=1, 1 do
  log_buffer_index = log_buffer_index+1
  print(log_buffer_index)
  log_buffer_data[log_buffer_index] = { attributes = { enforcer_descriptors_table, response_status_table } }
  log_buffer_index = log_buffer_index+1
  log_buffer_data[log_buffer_index] = { attributes = { enforcer_descriptors_table2, response_status_table } }
end

for i=1,log_buffer_index do
  table.insert(send_buffer,log_buffer_data[i] )
end

local my_table = {resource_logs={{scope_logs={{log_records= send_buffer } } } } }
print(json.encode(my_table))
-- Load proto
local p = protoc.new()
if not file_exists(proto_file) then
    print(("pb file: %s is not found"):format(proto_file))
end
-- local PROTOC_IMPORT_PATHS = {"."}
local import_paths = {"."}
-- for _, v in ipairs(PROTOC_IMPORT_PATHS or {}) do
--     table.insert(import_paths, v)
-- end
p.paths = import_paths
p.include_imports = true
print ("file name: " .. proto_file)

p:loadfile(proto_file)
if p.loaded then
    print (("pb file %s loaded succesefully"):format(proto_file))
end
local m = find_method(p.loaded, package, service, method)
if not m then
  print (("Undefined service method: %s/%s"):format(service, method))
end


-- local my_table = json.decode(message)
print(("Input proto type: %s"):format(m.input_type))
-- printTable(my_table)
local bytes = pb.encode(m.input_type, my_table)
local size = string.len(bytes)

-- Prepend gRPC specific prefix data
-- request is compressed (always 0)
-- request body size (4 bytes)
local prefix = {
    string.char(0),
    string.char(bit.band(bit.rshift(size, 24), 0xFF)),
    string.char(bit.band(bit.rshift(size, 16), 0xFF)),
    string.char(bit.band(bit.rshift(size, 8), 0xFF)),
    string.char(bit.band(size, 0xFF))
  }
local my_grpc_message = table.concat(prefix) .. bytes

local uri = ("http://%s:%s/%s"):format(host,tcp_port,otel_endpoint["path"])
local function sendH2_session(data, otel_endpoint)
  local http2 = require("resty.http2")
  print("module http2 loaded")
  local sock = socket.tcp()
  local ok, err = sock:connect("127.0.0.1", 31017)
  if not ok then
      print( err)
      return
  end
  local reuse_times = sock:getreusedtimes()
  print("conn reused  = " .. reuse_times)
  local opts = {}
  if reuse_times == 0 then
      opts = {
          ctx = sock,
          recv = sock.receive,
          send = sock.send,
      }
  else
      print("conn reused  NOT 0")
      opts = {
          ctx = sock,
          recv = sock.receive,
          send = sock.send,
          key = "radware",
      }
  end
  local headers = {
    { name = ":authority", value = otel_endpoint["host"] },
    { name = ":method", value = "POST" },
    { name = ":path", value = otel_endpoint["path"] },
    { name = ":scheme", value = "http" },
    { name = "accept-encoding", value = "deflate, gzip" },
    { name = "content-type", value = "application/grpc" },
    { name = "content-length", value = tostring(string.len(data)) },
  }
  local client, err = http2.new(opts)
  if not client then
      print( err)
      return
  end

  local ok, err = client:acknowledge_settings()
  if not ok then
      print( err)
      return
  end

  -- local ok, err = client:request(headers, data, on_headers_reach,
  --                                on_data_reach, on_trailers_reach)
  local stream, err = client:send_request(headers, data)
  print( "Send request")
  if not stream then
      print( "send request failed" .. err)
      return
  end
  print( "Before read buffer")
  local response_headers, err = client:read_headers(stream)
  if not  response_headers then
    print( " Failed to read response headers, error = " .. tostring(err))
  end
  print( "Before read body")
  local body, err = client:read_body(stream)
  if err then
    print( " Failed to read response body, error = " .. err)
  end
  print( "Before read trailers")
  -- local trailers, err = client:read_headers(stream)
  print( "After read trailers")
  if err then
    print( " Failed to read response trailers, error = " .. err)
  end
  -- local session = client.session
  -- local hd = h2_frame.header.new(8, h2_frame.PING_FRAME, 0, 0)
  -- local frame = {
  --     next = nil,
  --     header = hd,
  --     opaque_data_hi = 1234,
  --     opaque_data_lo = 5678,
  -- }

  -- session:frame_queue(frame)
  -- local ok, err = session:flush_queue()
  -- if not ok then
  --     print( err)
  --     return
  -- end

  -- local frame, err = session:recv_frame()
  -- if not frame then
  --     print( err)
  --     return
  -- end
  -- print(printTable(frame))
  local ok, err = sock:setkeepalive(0,1)
  if not ok then
      print( "setkeepalive failed, error = " .. err)
      sock:close()
      return
  end  
  print("Trying set H2 keepalive")
  client:keepalive("radware")
  print("Successefully set H2 keepalive")
end

for i=10,1,-1 do
  local hdl, err = sendH2_session(my_grpc_message, otel_endpoint)
end