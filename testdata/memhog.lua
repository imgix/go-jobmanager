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

local running = true
local leakyb = buffer()
local outb = buffer()
local c = 0
while running do
	c = c + 1
	if c == 900 and math.random(10) > 2 then
		--c = 0
		stderr:write("[memhog.lua] sleeping for 3secs\n")
		hub:sleep(30)
	end

	local err, line = instream:line("\n")
	if err and err == errors.CLOSED then 
		running = false

	else
		local n = 2000

		if c == 800 and math.random(10) > 4 then
			n = 60000
			c = 0
		end

		line = line .. " extra"
		for i=n,1,-1 do 
			leakyb:push(line)
		end
		local spref = ffi.new("pref_t", {#line})
		stdout:write(ffi.cast("char *", spref), ffi.sizeof(spref))
		stdout:write(line)
	end
	--hub:sleep(2)
end

