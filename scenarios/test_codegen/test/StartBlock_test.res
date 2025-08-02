open Belt
open RescriptMocha

describe("Start Block Per Contract Tests", () => {
  describe("Config Tests", () => {
    it(
      "should use contract-specific start block when provided",
      () => {
        let config = RegisterHandlers.getConfig()
        let chainConfig = config.chainMap->ChainMap.get(ChainMap.Chain.makeUnsafe(~chainId=1337))

        // Test Gravatar has start block 500
        let gravatarContract = chainConfig.contracts->Array.getBy(c => c.name === "Gravatar")
        switch gravatarContract {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            Some(500),
            ~message="Gravatar should have start block 500",
          )
        | None => Assert.fail("Gravatar contract not found")
        }

        // Test NftFactory has start block 1000
        let nftFactoryContract = chainConfig.contracts->Array.getBy(c => c.name === "NftFactory")
        switch nftFactoryContract {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            Some(1000),
            ~message="NftFactory should have start block 1000",
          )
        | None => Assert.fail("NftFactory contract not found")
        }

        // Test SimpleNft has no start block (should use network default)
        let simpleNftContract = chainConfig.contracts->Array.getBy(c => c.name === "SimpleNft")
        switch simpleNftContract {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            None,
            ~message="SimpleNft should have no start block (uses network default)",
          )
        | None => Assert.fail("SimpleNft contract not found")
        }

        // Test TestEvents has start block 2000
        let testEventsContract = chainConfig.contracts->Array.getBy(c => c.name === "TestEvents")
        switch testEventsContract {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            Some(2000),
            ~message="TestEvents should have start block 2000",
          )
        | None => Assert.fail("TestEvents contract not found")
        }
      },
    )

    it(
      "should use network start block as fallback",
      () => {
        let config = RegisterHandlers.getConfig()
        let chainConfig = config.chainMap->ChainMap.get(ChainMap.Chain.makeUnsafe(~chainId=1))

        // Network start block is 1
        Assert.equal(
          chainConfig.startBlock,
          1,
          ~message="Chain 1 should have network start block 1",
        )

        // Test Noop has specific start block 100
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
      },
    )
  })

  describe("FetchState Start Block Tests", () => {
    let mockAddress0 = TestHelpers.Addresses.mockAddresses[0]->Option.getExn
    let mockAddress1 = TestHelpers.Addresses.mockAddresses[1]->Option.getExn
    let mockAddress2 = TestHelpers.Addresses.mockAddresses[2]->Option.getExn

    it(
      "should create indexing contracts with correct start blocks",
      () => {
        let staticContracts = Js.Dict.fromArray([
          ("TestContract", [mockAddress0, mockAddress1]),
          ("AnotherContract", [mockAddress2]),
        ])

        let staticContractsWithStartBlocks = Js.Dict.fromArray([
          ("TestContract", Some(500)),
          ("AnotherContract", None), // Should use network start block
        ])

        let eventConfigs = [
          (Mock.evmEventConfig(~id="0", ~contractName="TestContract") :> Internal.eventConfig),
          (Mock.evmEventConfig(~id="1", ~contractName="AnotherContract") :> Internal.eventConfig),
        ]

        let fetchState = FetchState.make(
          ~eventConfigs,
          ~staticContracts,
          ~staticContractsWithStartBlocks,
          ~dynamicContracts=[],
          ~startBlock=1,
          ~endBlock=None,
          ~maxAddrInPartition=3,
          ~chainId=1337,
        )

        // Check that TestContract addresses have start block 500
        let testContractAddress0 =
          fetchState.indexingContracts->Utils.Dict.dangerouslyGetNonOption(
            mockAddress0->Address.toString,
          )
        switch testContractAddress0 {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            500,
            ~message="TestContract address 0 should have start block 500",
          )
        | None => Assert.fail("TestContract address 0 not found in indexing contracts")
        }

        let testContractAddress1 =
          fetchState.indexingContracts->Utils.Dict.dangerouslyGetNonOption(
            mockAddress1->Address.toString,
          )
        switch testContractAddress1 {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            500,
            ~message="TestContract address 1 should have start block 500",
          )
        | None => Assert.fail("TestContract address 1 not found in indexing contracts")
        }

        // Check that AnotherContract address uses network start block (1)
        let anotherContractAddress =
          fetchState.indexingContracts->Utils.Dict.dangerouslyGetNonOption(
            mockAddress2->Address.toString,
          )
        switch anotherContractAddress {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            1,
            ~message="AnotherContract should use network start block 1",
          )
        | None => Assert.fail("AnotherContract address not found in indexing contracts")
        }
      },
    )

    it(
      "should handle dynamic contracts with start blocks",
      () => {
        let eventConfigs = [
          (Mock.evmEventConfig(~id="0", ~contractName="DynamicContract") :> Internal.eventConfig),
        ]

        let dc: FetchState.indexingContract = {
          address: mockAddress0,
          contractName: "DynamicContract",
          startBlock: 1500, // Dynamic contract specific start block
          register: DC({
            registeringEventLogIndex: 0,
            registeringEventBlockTimestamp: 15000,
            registeringEventContractName: "Factory",
            registeringEventName: "ContractCreated",
            registeringEventSrcAddress: mockAddress1,
          }),
        }

        let fetchState = FetchState.make(
          ~eventConfigs,
          ~staticContracts=Js.Dict.empty(),
          ~staticContractsWithStartBlocks=Js.Dict.empty(),
          ~dynamicContracts=[dc],
          ~startBlock=1,
          ~endBlock=None,
          ~maxAddrInPartition=3,
          ~chainId=1337,
        )

        // Check that dynamic contract has its specific start block
        let dynamicContractFound =
          fetchState.indexingContracts->Utils.Dict.dangerouslyGetNonOption(
            mockAddress0->Address.toString,
          )
        switch dynamicContractFound {
        | Some(contract) =>
          Assert.equal(
            contract.startBlock,
            1500,
            ~message="Dynamic contract should have start block 1500",
          )
        | None => Assert.fail("Dynamic contract not found in indexing contracts")
        }
      },
    )
  })

  describe("EventRouter Start Block Filtering Tests", () => {
    let mockAddress = TestHelpers.Addresses.mockAddresses[0]->Option.getExn

    let createTestIndexingContracts = (~startBlock) => {
      let dict = Js.Dict.empty()
      dict->Js.Dict.set(
        mockAddress->Address.toString,
        {
          FetchState.address: mockAddress,
          contractName: "TestContract",
          startBlock,
          register: Config,
        },
      )
      dict
    }

    it(
      "should filter events based on contract start block",
      () => {
        let router = EventRouter.empty()
        let eventConfig = Mock.evmEventConfig(~id="test_event", ~contractName="TestContract")

        router->EventRouter.addOrThrow(
          eventConfig.id,
          eventConfig,
          ~contractName="TestContract",
          ~eventName="TestEvent",
          ~chain=ChainMap.Chain.makeUnsafe(~chainId=1337),
          ~isWildcard=false,
        )

        let indexingContracts = createTestIndexingContracts(~startBlock=500)

        // Event at block 400 should be filtered out (before start block)
        let result400 =
          router->EventRouter.get(
            ~tag=eventConfig.id,
            ~contractAddress=mockAddress,
            ~blockNumber=400,
            ~indexingContracts,
          )
        Assert.equal(
          result400,
          None,
          ~message="Event at block 400 should be filtered out (before start block 500)",
        )

        // Event at block 500 should be included (at start block)
        let result500 =
          router->EventRouter.get(
            ~tag=eventConfig.id,
            ~contractAddress=mockAddress,
            ~blockNumber=500,
            ~indexingContracts,
          )
        Assert.notEqual(
          result500,
          None,
          ~message="Event at block 500 should be included (at start block)",
        )

        // Event at block 600 should be included (after start block)
        let result600 =
          router->EventRouter.get(
            ~tag=eventConfig.id,
            ~contractAddress=mockAddress,
            ~blockNumber=600,
            ~indexingContracts,
          )
        Assert.notEqual(
          result600,
          None,
          ~message="Event at block 600 should be included (after start block)",
        )
      },
    )

    it(
      "should handle wildcard events with start blocks",
      () => {
        let router = EventRouter.empty()
        let wildcardEventConfig = Mock.evmEventConfig(
          ~id="wildcard_event",
          ~contractName="TestContract",
          ~isWildcard=true,
        )

        router->EventRouter.addOrThrow(
          wildcardEventConfig.id,
          wildcardEventConfig,
          ~contractName="TestContract",
          ~eventName="WildcardEvent",
          ~chain=ChainMap.Chain.makeUnsafe(~chainId=1337),
          ~isWildcard=true,
        )

        let indexingContracts = createTestIndexingContracts(~startBlock=1000)

        // Test with unknown address (should use wildcard but still respect start block logic)
        let unknownAddress = TestHelpers.Addresses.mockAddresses[1]->Option.getExn

        // Event before start block should still be filtered for wildcard
        let resultBefore =
          router->EventRouter.get(
            ~tag=wildcardEventConfig.id,
            ~contractAddress=unknownAddress,
            ~blockNumber=500,
            ~indexingContracts,
          )
        // Wildcard should still work for unknown addresses
        Assert.notEqual(
          resultBefore,
          None,
          ~message="Wildcard event should work for unknown addresses",
        )
      },
    )
  })
})
