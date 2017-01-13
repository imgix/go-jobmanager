local levee = require("levee")
local _ = levee._
local math = require("math")

local buffer = require("levee").d.Buffer

local errors = require("levee.errors")

local ffi = require("ffi")
local C = ffi.C

ffi.cdef([[
typedef struct { unsigned short len; } pref_t;
]])


local hub = levee.Hub()
local stdin = hub.io:stdin()
local instream = stdin:stream()

local stdout = hub.io:stdout()
_.fcntl_nonblock(2)
local stderr = hub.io:w(2)

print("ARGS: ", arg)
local os = require("os")
local argv = _.argv(arg)

local sleep_delay = 10 * tonumber(argv:next())

local running = true
while running do
	local err, line = instream:line("\n")
	if err and err == errors.CLOSED then 
		running = false
	else
		stderr:write(string.format("START sleeping %dms\n", sleep_delay))
		hub:sleep(sleep_delay)
		stderr:write(string.format("DONE sleeping %dms\n", sleep_delay))
		line = line .. " extra"
		local spref = ffi.new("pref_t", {#line})
		stdout:write(ffi.cast("char *", spref), ffi.sizeof(spref))
		stdout:write(line)
	end
end

