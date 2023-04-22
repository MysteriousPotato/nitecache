<!-- PROJECT LOGO -->

<br />
<div id="readme-top" style="text-align: center">
  <a href="images/MysteriousPotato/nitecache">
    <img src="images/logo.png" alt="Logo" height="100">
  </a>
    <h1>nitecache</h1>
    golang cache library
</div>


<!-- TABLE OF CONTENTS -->

# Table of Contents
- [Getting started](#getting-started">)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)


<!-- ABOUT THE PROJECT -->

## About The Project

***disclaimer*** : This project is still in its experimental phase.

nitecache is an embedded and distributed cache library for golang that supports:
- sharding
- explicit cache eviction
- ttl
- lfu/lru eviction policies
- updating keys using golang functions
- grpc
- type safety using generics

<p style="text-align: right">(<a href="#readme-top">back to top</a>)</p>


<!-- GETTING STARTED -->

## Getting started

### Installation
```sh
go get github.com/MysteriousPotato/nitecache
```

### Usage

##### Creating a cache instance:
``` go
// Both ID and Addr must be unique across peers
selfID := "1"
members := []nitecache.Member{
    {ID: "1", Addr: "node1:8100"},
    {ID: "2", Addr: "node2:8100"},
}

c, err := nitecache.NewCache(
    selfID,
    members,
    nitecache.VirtualNodeOpt(64),
    nitecache.TimeoutOpt(time.Second*5),
    nitecache.HashFuncOpt(
        func(key string) (int, error) {
            return int(crc32.ChecksumIEEE([]byte(key))), nil
        },
    ),
)
if err != nil {
    panic(err)
}
defer func() {
    if err := c.TearDown(); err != nil {
        panic(err)
    }
}()
```

##### Creating a table:
``` go
// Specify the name of the table
table := nitecache.NewTable[string]("key-value-store").
    // If WithEvictionPolicy is omitted, nitecache won't apply any eviction policy
    WithEvictionPolicy(nitecache.NewLruPolicy(256<<20)).
    // Option to specify the cache-aside getter
    // If WithGetter is omitted, nitecache will return an error on cache miss. 
    WithGetter(
        func(key string) (Session, time.Duration, error) {
            sess, err := getSessionFromSomewhere()
            if err != nil {
                return Session{}, 0, err
            }
            //Return the value and a ttl (optional)
            return sess, time.Hour, nil
        },
    ).
    Build(c) // Pass cache instance to Build method
```

##### Retrieving a value by key:
``` go
// If no corresponding value is found and no cache-aside getter was provided, returns ErrKeyNotFound.
value, err := table.Get(ctx, "key")
if err != nil {
}
```

##### Retrieving a value by key:
``` go
// If no corresponding value is found and no cache-aside getter was provided, returns ErrKeyNotFound.
value, err := table.Get(ctx, "key")
if err != nil {
}
```
##### Create a new entry:
``` go
if err := table.Put(ctx, "key", sess, time.Hour); err != nil {
}
```

##### Evicting a value by key:
``` go
if err := table.Evict(ctx, "key"); err != nil {
}
```

##### Registering and using a function to update a value:
``` go
// WithFunction is used to register the function.
table := nitecache.NewTable[RateLimitEntry]("rate-limiter").
    WithFunction(
        "updateUsername", func(r RateLimitEntry, args []byte) (Session, time.Duration, error) {
            r.Count++
            return r, 0, nil
        },
    ).
    Build(c)
    
// Executes previously registered function "updateUsername".
// You can pass arguments as bytes to the function call for more flexibility.
// This can be useful if you need previous state to properly update a value.
sess, err = table.Execute(ctx, "key", "updateUsername", []byte("new username"))
if err != nil {
}
```

<p style="text-align: right">(<a href="#readme-top">back to top</a>)</p>


<!-- ROADMAP -->

## Roadmap

See the [open issues](https://github.com/MysteriousPotato/nitecache/issues) for a full list of proposed features (and known issues).

<p style="text-align: right">(<a href="#readme-top">back to top</a>)</p>


<!-- CONTRIBUTING -->

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".

<p style="text-align: right">(<a href="#readme-top">back to top</a>)</p>


<!-- LICENSE -->

## License

Distributed under the MIT License. See [LICENSE](https://github.com/MysteriousPotato/nitecache/blob/master/LICENSE) for more information.

<p style="text-align: right">(<a href="#readme-top">back to top</a>)</p>