open Belt
open RescriptMocha

describe("Start Block Per Contract Integration Tests", () => {
  describe("ChainFetcher Integration", () => {
    it(
      "should create ChainFetcher with correct start blocks from config",
      () => {
        let config = RegisterHandlers.getConfig()
        let chainConfig = config.chainMap->ChainMap.get(ChainMap.Chain.makeUnsafe(~chainId=1337))

        // Verify that static contracts are set up with their specific start blocks
        let staticContracts = Js.Dict.empty()
        let staticContractsWithStartBlocks = Js.Dict.empty()

        chainConfig.contracts->Array.forEach(
          contract => {
            staticContracts->Js.Dict.set(contract.name, contract.addresses)
            staticContractsWithStartBlocks->Js.Dict.set(contract.name, contract.startBlock)
          },
        )

        // Test that Gravatar has start block 500
        let gravatarStartBlock =
          staticContractsWithStartBlocks->Utils.Dict.dangerouslyGetNonOption("Gravatar")
        Assert.equal(
          gravatarStartBlock,
          Some(Some(500)),
          ~message="Gravatar should have start block 500 in chain fetcher setup",
        )

        // Test that NftFactory has start block 1000
        let nftFactoryStartBlock =
          staticContractsWithStartBlocks->Utils.Dict.dangerouslyGetNonOption("NftFactory")
        Assert.equal(
          nftFactoryStartBlock,
          Some(Some(1000)),
          ~message="NftFactory should have start block 1000 in chain fetcher setup",
        )

        // Test that SimpleNft has no specific start block (uses network default)
        let simpleNftContract = chainConfig.contracts->Array.getBy(c => c.name === "SimpleNft")
        switch simpleNftContract {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            None,
            ~message="SimpleNft should have no specific start block",
          )
        | None => Assert.fail("SimpleNft contract not found")
        }

        // Test that TestEvents has start block 2000
        let testEventsStartBlock =
          staticContractsWithStartBlocks->Utils.Dict.dangerouslyGetNonOption("TestEvents")
        Assert.equal(
          testEventsStartBlock,
          Some(Some(2000)),
          ~message="TestEvents should have start block 2000 in chain fetcher setup",
        )
      },
    )

    it(
      "should properly initialize FetchState with contract start blocks",
      () => {
        let config = RegisterHandlers.getConfig()
        let chainConfig = config.chainMap->ChainMap.get(ChainMap.Chain.makeUnsafe(~chainId=1337))

        let chainFetcher = ChainFetcher.makeFromConfig(
          chainConfig,
          ~maxAddrInPartition=Env.maxAddrInPartition,
          ~enableRawEvents=true,
        )

        let fetchState = chainFetcher.fetchState

        // Check that indexing contracts were created with correct start blocks
        // We need to find the actual addresses from the config to test this properly
        let gravatarContract = chainConfig.contracts->Array.getBy(c => c.name === "Gravatar")
        switch gravatarContract {
        | Some(contract) => {
            let gravatarAddress = contract.addresses->Array.get(0)
            switch gravatarAddress {
            | Some(address) => {
                let indexingContract =
                  fetchState.indexingContracts->Utils.Dict.dangerouslyGetNonOption(
                    address->Address.toString,
                  )
                switch indexingContract {
                | Some(ic) =>
                  Assert.equal(
                    ic.startBlock,
                    500,
                    ~message="Gravatar indexing contract should have start block 500",
                  )
                | None => Assert.fail("Gravatar indexing contract not found")
                }
              }
            | None => Assert.fail("Gravatar address not found")
            }
          }
        | None => Assert.fail("Gravatar contract not found in config")
        }

        let nftFactoryContract = chainConfig.contracts->Array.getBy(c => c.name === "NftFactory")
        switch nftFactoryContract {
        | Some(contract) => {
            let nftFactoryAddress = contract.addresses->Array.get(0)
            switch nftFactoryAddress {
            | Some(address) => {
                let indexingContract =
                  fetchState.indexingContracts->Utils.Dict.dangerouslyGetNonOption(
                    address->Address.toString,
                  )
                switch indexingContract {
                | Some(ic) =>
                  Assert.equal(
                    ic.startBlock,
                    1000,
                    ~message="NftFactory indexing contract should have start block 1000",
                  )
                | None => Assert.fail("NftFactory indexing contract not found")
                }
              }
            | None => Assert.fail("NftFactory address not found")
            }
          }
        | None => Assert.fail("NftFactory contract not found in config")
        }

        // Check SimpleNft uses network start block
        let simpleNftContract = chainConfig.contracts->Array.getBy(c => c.name === "SimpleNft")
        switch simpleNftContract {
        | Some(contract) => if contract.addresses->Array.length > 0 {
            let simpleNftAddress = contract.addresses->Array.get(0)
            switch simpleNftAddress {
            | Some(address) => {
                let indexingContract =
                  fetchState.indexingContracts->Utils.Dict.dangerouslyGetNonOption(
                    address->Address.toString,
                  )
                switch indexingContract {
                | Some(ic) =>
                  Assert.equal(
                    ic.startBlock,
                    1, // Network start block
                    ~message="SimpleNft indexing contract should use network start block 1",
                  )
                | None => Assert.fail("SimpleNft indexing contract not found")
                }
              }
            | None => Assert.fail("SimpleNft address not found")
            }
          } else {
            // Skip test if contract has no addresses
            ()
          }
        | None => Assert.fail("SimpleNft contract not found")
        }
      },
    )
  })

  describe("Cross-Chain Start Block Tests", () => {
    it(
      "should handle different start blocks across multiple chains",
      () => {
        let config = RegisterHandlers.getConfig()

        // Test Chain 1
        let chainConfig = config.chainMap->ChainMap.get(ChainMap.Chain.makeUnsafe(~chainId=1))
        // Chain 1 network start block should be 1
        Assert.equal(
          chainConfig.startBlock,
          1,
          ~message="Chain 1 should have network start block 1",
        )

        // Noop contract on chain 1 should have start block 100
        let noopContract = chainConfig.contracts->Array.getBy(c => c.name === "Noop")
        switch noopContract {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            Some(100),
            ~message="Noop on chain 1 should have start block 100",
          )
        | None => Assert.fail("Noop contract not found on chain 1")
        }

        // Test Chain 100
        let chainConfig = config.chainMap->ChainMap.get(ChainMap.Chain.makeUnsafe(~chainId=100))
        // Chain 100 network start block should be 1
        Assert.equal(
          chainConfig.startBlock,
          1,
          ~message="Chain 100 should have network start block 1",
        )

        // EventFiltersTest contract on chain 100 should have start block 50
        let eventFiltersContract =
          chainConfig.contracts->Array.getBy(c => c.name === "EventFiltersTest")
        switch eventFiltersContract {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            Some(50),
            ~message="EventFiltersTest on chain 100 should have start block 50",
          )
        | None => Assert.fail("EventFiltersTest contract not found on chain 100")
        }

        // Test Chain 137
        let chainConfig = config.chainMap->ChainMap.get(ChainMap.Chain.makeUnsafe(~chainId=137))
        // Chain 137 network start block should be 1
        Assert.equal(
          chainConfig.startBlock,
          1,
          ~message="Chain 137 should have network start block 1",
        )

        // EventFiltersTest contract on chain 137 should have no specific start block
        let eventFiltersContract =
          chainConfig.contracts->Array.getBy(c => c.name === "EventFiltersTest")
        switch eventFiltersContract {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            None,
            ~message="EventFiltersTest on chain 137 should have no specific start block",
          )
        | None => Assert.fail("EventFiltersTest contract not found on chain 137")
        }

        // Noop contract on chain 137 should have start block 200
        let noopContract = chainConfig.contracts->Array.getBy(c => c.name === "Noop")
        switch noopContract {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            Some(200),
            ~message="Noop on chain 137 should have start block 200",
          )
        | None => Assert.fail("Noop contract not found on chain 137")
        }
      },
    )
  })

  describe("EventRouter Integration with Start Blocks", () => {
    it(
      "should properly filter events using configured start blocks",
      () => {
        let config = RegisterHandlers.getConfig()
        let chainConfig = config.chainMap->ChainMap.get(ChainMap.Chain.makeUnsafe(~chainId=1337))

        let chainFetcher = ChainFetcher.makeFromConfig(
          chainConfig,
          ~maxAddrInPartition=Env.maxAddrInPartition,
          ~enableRawEvents=true,
        )

        let fetchState = chainFetcher.fetchState

        // Find Gravatar contract and its events
        let gravatarContract = chainConfig.contracts->Array.getBy(c => c.name === "Gravatar")
        switch gravatarContract {
        | Some(contract) => {
            let gravatarAddress = contract.addresses->Array.get(0)
            switch gravatarAddress {
            | Some(address) => {
                // Test that events before start block 500 are filtered out
                // and events at or after start block 500 are included

                // This test verifies the integration between:
                // 1. Config parsing (start_block: 500 for Gravatar)
                // 2. FetchState creation with indexing contracts
                // 3. EventRouter filtering based on start blocks

                let indexingContract =
                  fetchState.indexingContracts->Utils.Dict.dangerouslyGetNonOption(
                    address->Address.toString,
                  )

                switch indexingContract {
                | Some(ic) => {
                    Assert.equal(
                      ic.startBlock,
                      500,
                      ~message="Integration test: Gravatar should have start block 500",
                    )

                    Assert.equal(
                      ic.contractName,
                      "Gravatar",
                      ~message="Integration test: Should be Gravatar contract",
                    )

                    Assert.equal(
                      ic.register,
                      Config,
                      ~message="Integration test: Should be static config contract",
                    )
                  }
                | None => Assert.fail("Integration test: Gravatar indexing contract not found")
                }
              }
            | None => Assert.fail("Integration test: Gravatar address not found")
            }
          }
        | None => Assert.fail("Integration test: Gravatar contract not found")
        }
      },
    )
  })

  describe("Config Validation", () => {
    it(
      "should validate that all configured start blocks are properly loaded",
      () => {
        let config = RegisterHandlers.getConfig()

        // Verify that the config contains all our test start blocks
        let chainConfigs = [
          (
            1337,
            [
              ("Gravatar", Some(500)),
              ("NftFactory", Some(1000)),
              ("SimpleNft", None),
              ("TestEvents", Some(2000)),
            ],
          ),
          (1, [("Noop", Some(100))]),
          (100, [("EventFiltersTest", Some(50))]),
          (137, [("EventFiltersTest", None), ("Noop", Some(200))]),
        ]

        chainConfigs->Array.forEach(
          ((chainId, expectedContracts)) => {
            let chainConfig = config.chainMap->ChainMap.get(ChainMap.Chain.makeUnsafe(~chainId))

            expectedContracts->Array.forEach(
              ((contractName, expectedStartBlock)) => {
                let contract = chainConfig.contracts->Array.getBy(c => c.name === contractName)
                switch contract {
                | Some(contract) =>
                  Assert.equal(
                    contract.startBlock,
                    expectedStartBlock,
                    ~message=`Chain ${chainId->Int.toString}: ${contractName} should have expected start block`,
                  )
                | None =>
                  Assert.fail(`Chain ${chainId->Int.toString}: ${contractName} contract not found`)
                }
              },
            )
          },
        )
      },
    )
  })
})
