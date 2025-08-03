open Belt

type dcData = {
  registeringEventBlockTimestamp: int,
  registeringEventLogIndex: int,
  registeringEventContractName: string,
  registeringEventName: string,
  registeringEventSrcAddress: Address.t,
}

@unboxed
type contractRegister =
  | Config
  | DC(dcData)
type indexingContract = {
  address: Address.t,
  contractName: string,
  startBlock: int,
  register: contractRegister,
}

type contractConfig = {filterByAddresses: bool}

type blockNumberAndTimestamp = {
  blockNumber: int,
  blockTimestamp: int,
}

type blockNumberAndLogIndex = {blockNumber: int, logIndex: int}

type selection = {eventConfigs: array<Internal.eventConfig>, dependsOnAddresses: bool}

type status = {mutable fetchingStateId: option<int>}

/**
A state that holds a queue of events and data regarding what to fetch next
for specific contract events with a given contract address.
When partitions for the same events are caught up to each other
the are getting merged until the maxAddrInPartition is reached.
*/
type partition = {
  id: string,
  status: status,
  latestFetchedBlock: blockNumberAndTimestamp,
  selection: selection,
  addressesByContractName: dict<array<Address.t>>,
}

type t = {
  partitions: array<partition>,
  // Used for the incremental partition id. Can't use the partitions length,
  // since partitions might be deleted on merge or cleaned up
  nextPartitionIndex: int,
  isFetchingAtHead: bool,
  endBlock: option<int>,
  maxAddrInPartition: int,
  firstEventBlockNumber: option<int>,
  normalSelection: selection,
  // By address
  indexingContracts: dict<indexingContract>,
  // By contract name
  contractConfigs: dict<contractConfig>,
  // Registered dynamic contracts that need to be stored in the db
  // Should read them at the same time when getting items for the batch
  dcsToStore: option<array<indexingContract>>,
  // Not used for logic - only metadata
  chainId: int,
  // Fields computed by updateInternal
  latestFullyFetchedBlock: blockNumberAndTimestamp,
  // How much blocks behind the head we should query
  // Added for the purpose of avoiding reorg handling
  blockLag: option<int>,
  //Items ordered from latest to earliest
  queue: array<Internal.eventItem>,
}

let copy = (fetchState: t) => {
  {
    maxAddrInPartition: fetchState.maxAddrInPartition,
    partitions: fetchState.partitions,
    endBlock: fetchState.endBlock,
    nextPartitionIndex: fetchState.nextPartitionIndex,
    isFetchingAtHead: fetchState.isFetchingAtHead,
    latestFullyFetchedBlock: fetchState.latestFullyFetchedBlock,
    normalSelection: fetchState.normalSelection,
    firstEventBlockNumber: fetchState.firstEventBlockNumber,
    chainId: fetchState.chainId,
    contractConfigs: fetchState.contractConfigs,
    indexingContracts: fetchState.indexingContracts,
    dcsToStore: fetchState.dcsToStore,
    blockLag: fetchState.blockLag,
    queue: fetchState.queue->Array.copy,
  }
}

/*
Comapritor for two events from the same chain. No need for chain id or timestamp
*/
let eventItemGt = (a: Internal.eventItem, b: Internal.eventItem) =>
  if a.blockNumber > b.blockNumber {
    true
  } else if a.blockNumber === b.blockNumber {
    a.logIndex > b.logIndex
  } else {
    false
  }

/*
Merges two event queues on a single event fetcher

Pass the shorter list into A for better performance
*/
let mergeSortedEventList = (a, b) => Utils.Array.mergeSorted(eventItemGt, a, b)

let mergeIntoPartition = (p: partition, ~target: partition, ~maxAddrInPartition) => {
  switch (p, target) {
  | ({selection: {dependsOnAddresses: true}}, {selection: {dependsOnAddresses: true}}) => {
      let latestFetchedBlock = target.latestFetchedBlock

      let mergedAddresses = Js.Dict.empty()

      let allowedAddressesNumber = ref(maxAddrInPartition)

      target.addressesByContractName->Utils.Dict.forEachWithKey((contractName, addresses) => {
        allowedAddressesNumber := allowedAddressesNumber.contents - addresses->Array.length
        mergedAddresses->Js.Dict.set(contractName, addresses)
      })

      // Start with putting all addresses to the merging dict
      // And if they exceed the limit, start removing from the merging dict
      // and putting into the rest dict
      p.addressesByContractName->Utils.Dict.forEachWithKey((contractName, addresses) => {
        allowedAddressesNumber := allowedAddressesNumber.contents - addresses->Array.length
        switch mergedAddresses->Utils.Dict.dangerouslyGetNonOption(contractName) {
        | Some(targetAddresses) =>
          mergedAddresses->Js.Dict.set(contractName, Array.concat(targetAddresses, addresses))
        | None => mergedAddresses->Js.Dict.set(contractName, addresses)
        }
      })

      let rest = if allowedAddressesNumber.contents < 0 {
        let restAddresses = Js.Dict.empty()

        mergedAddresses->Utils.Dict.forEachWithKey((contractName, addresses) => {
          if allowedAddressesNumber.contents === 0 {
            ()
          } else if addresses->Array.length <= -allowedAddressesNumber.contents {
            allowedAddressesNumber := allowedAddressesNumber.contents + addresses->Array.length
            mergedAddresses->Utils.Dict.deleteInPlace(contractName)
            restAddresses->Js.Dict.set(contractName, addresses)
          } else {
            let restFrom = addresses->Array.length + allowedAddressesNumber.contents
            mergedAddresses->Js.Dict.set(
              contractName,
              addresses->Js.Array2.slice(~start=0, ~end_=restFrom),
            )
            restAddresses->Js.Dict.set(contractName, addresses->Js.Array2.sliceFrom(restFrom))
            allowedAddressesNumber := 0
          }
        })

        Some({
          id: p.id,
          status: {
            fetchingStateId: None,
          },
          selection: target.selection,
          addressesByContractName: restAddresses,
          latestFetchedBlock,
        })
      } else {
        None
      }

      (
        {
          id: target.id,
          status: {
            fetchingStateId: None,
          },
          selection: target.selection,
          addressesByContractName: mergedAddresses,
          latestFetchedBlock,
        },
        rest,
      )
    }
  | ({selection: {dependsOnAddresses: false}}, _)
  | (_, {selection: {dependsOnAddresses: false}}) => (p, Some(target))
  }
}

/* strategy for TUI synced status:
 * Firstly -> only update synced status after batch is processed (not on batch creation). But also set when a batch tries to be created and there is no batch
 *
 * Secondly -> reset timestampCaughtUpToHead and isFetching at head when dynamic contracts get registered to a chain if they are not within 0.001 percent of the current block height
 *
 * New conditions for valid synced:
 *
 * CASE 1 (chains are being synchronised at the head)
 *
 * All chain fetchers are fetching at the head AND
 * No events that can be processed on the queue (even if events still exist on the individual queues)
 * CASE 2 (chain finishes earlier than any other chain)
 *
 * CASE 3 endblock has been reached and latest processed block is greater than or equal to endblock (both fields must be Some)
 *
 * The given chain fetcher is fetching at the head or latest processed block >= endblock
 * The given chain has processed all events on the queue
 * see https://github.com/Float-Capital/indexer/pull/1388 */

/* Dynamic contracts pose a unique case when calculated whether a chain is synced or not.
 * Specifically, in the initial syncing state from SearchingForEvents -> Synced, where although a chain has technically processed up to all blocks
 * for a contract that emits events with dynamic contracts, it is possible that those dynamic contracts will need to be indexed from blocks way before
 * the current block height. This is a toleration check where if there are dynamic contracts within a batch, check how far are they from the currentblock height.
 * If it is less than 1 thousandth of a percent, then we deem that contract to be within the synced range, and therefore do not reset the synced status of the chain */
let checkIsWithinSyncRange = (~latestFetchedBlock: blockNumberAndTimestamp, ~currentBlockHeight) =>
  (currentBlockHeight->Int.toFloat -. latestFetchedBlock.blockNumber->Int.toFloat) /.
    currentBlockHeight->Int.toFloat <= 0.001

/*
 Update fetchState, merge registers and recompute derived values
 */
let updateInternal = (
  fetchState: t,
  ~partitions=fetchState.partitions,
  ~nextPartitionIndex=fetchState.nextPartitionIndex,
  ~indexingContracts=fetchState.indexingContracts,
  ~dcsToStore=fetchState.dcsToStore,
  ~currentBlockHeight=?,
  ~queue=fetchState.queue,
): t => {
  let firstPartition = partitions->Js.Array2.unsafe_get(0)
  let latestFullyFetchedBlock = ref(firstPartition.latestFetchedBlock)
  for idx in 0 to partitions->Array.length - 1 {
    let p = partitions->Js.Array2.unsafe_get(idx)
    if latestFullyFetchedBlock.contents.blockNumber > p.latestFetchedBlock.blockNumber {
      latestFullyFetchedBlock := p.latestFetchedBlock
    }
  }
  let latestFullyFetchedBlock = latestFullyFetchedBlock.contents

  let isFetchingAtHead = switch currentBlockHeight {
  | None => fetchState.isFetchingAtHead
  | Some(currentBlockHeight) =>
    // Sync isFetchingAtHead when currentBlockHeight is provided
    if latestFullyFetchedBlock.blockNumber >= currentBlockHeight {
      true
    } else if (
      // For dc registration reset the state only when dcs are not in the sync range
      fetchState.isFetchingAtHead &&
      checkIsWithinSyncRange(~latestFetchedBlock=latestFullyFetchedBlock, ~currentBlockHeight)
    ) {
      true
    } else {
      false
    }
  }

  let queueSize = queue->Array.length
  Prometheus.IndexingPartitions.set(
    ~partitionsCount=partitions->Array.length,
    ~chainId=fetchState.chainId,
  )
  Prometheus.IndexingBufferSize.set(~bufferSize=queueSize, ~chainId=fetchState.chainId)
  Prometheus.IndexingBufferBlockNumber.set(
    ~blockNumber=latestFullyFetchedBlock.blockNumber,
    ~chainId=fetchState.chainId,
  )

  {
    maxAddrInPartition: fetchState.maxAddrInPartition,
    endBlock: fetchState.endBlock,
    contractConfigs: fetchState.contractConfigs,
    normalSelection: fetchState.normalSelection,
    chainId: fetchState.chainId,
    nextPartitionIndex,
    firstEventBlockNumber: switch queue->Utils.Array.last {
    | Some(item) => Utils.Math.minOptInt(fetchState.firstEventBlockNumber, Some(item.blockNumber))
    | None => fetchState.firstEventBlockNumber
    },
    partitions,
    isFetchingAtHead,
    latestFullyFetchedBlock,
    indexingContracts,
    dcsToStore,
    blockLag: fetchState.blockLag,
    queue,
  }
}

let numAddresses = fetchState => fetchState.indexingContracts->Js.Dict.keys->Array.length

let warnDifferentContractType = (fetchState, ~existingContract, ~dc: indexingContract) => {
  let logger = Logging.createChild(
    ~params={
      "chainId": fetchState.chainId,
      "contractAddress": dc.address->Address.toString,
      "existingContractType": existingContract.contractName,
      "newContractType": dc.contractName,
    },
  )
  logger->Logging.childWarn(`Skipping contract registration: Contract address is already registered for one contract and cannot be registered for another contract.`)
}

let registerDynamicContracts = (
  fetchState: t,
  // These are raw dynamic contracts received from contractRegister call.
  // Might contain duplicates which we should filter out
  dynamicContracts: array<indexingContract>,
  ~currentBlockHeight,
  ~contractStartBlocks: dict<option<int>>=Js.Dict.empty(),
) => {
  if fetchState.normalSelection.eventConfigs->Utils.Array.isEmpty {
    // Can the normalSelection be empty?
    // Probably only on pre-registration, but we don't
    // register dynamic contracts during it
    Js.Exn.raiseError(
      "Invalid configuration. No events to fetch for the dynamic contract registration.",
    )
  }

  let indexingContracts = fetchState.indexingContracts
  let registeringContracts = Js.Dict.empty()
  let addressesByContractName = Js.Dict.empty()
  let earliestRegisteringEventBlockNumber = ref(%raw(`Infinity`))
  let hasDCWithFilterByAddresses = ref(false)

  for idx in 0 to dynamicContracts->Array.length - 1 {
    let dc = dynamicContracts->Js.Array2.unsafe_get(idx)
    switch fetchState.contractConfigs->Utils.Dict.dangerouslyGetNonOption(dc.contractName) {
    | Some({filterByAddresses}) =>
      // Prevent registering already indexing contracts
      switch indexingContracts->Utils.Dict.dangerouslyGetNonOption(dc.address->Address.toString) {
      | Some(existingContract) =>
        // FIXME: Instead of filtering out duplicates,
        // we should check the block number first.
        // If new registration with earlier block number
        // we should register it for the missing block range
        if existingContract.contractName != dc.contractName {
          fetchState->warnDifferentContractType(~existingContract, ~dc)
        } else if existingContract.startBlock > dc.startBlock {
          let logger = Logging.createChild(
            ~params={
              "chainId": fetchState.chainId,
              "contractAddress": dc.address->Address.toString,
              "existingBlockNumber": existingContract.startBlock,
              "newBlockNumber": dc.startBlock,
            },
          )
          logger->Logging.childWarn(`Skipping contract registration: Contract address is already registered at a later block number. Currently registration of the same contract address is not supported by Envio. Reach out to us if it's a problem for you.`)
        }
        ()
      | None =>
        let shouldUpdate = switch registeringContracts->Utils.Dict.dangerouslyGetNonOption(
          dc.address->Address.toString,
        ) {
        | Some(registeringContract) if registeringContract.contractName != dc.contractName =>
          fetchState->warnDifferentContractType(~existingContract=registeringContract, ~dc)
          false
        | Some(registeringContract) =>
          switch (registeringContract.register, dc.register) {
          | (
              DC({registeringEventLogIndex}),
              DC({registeringEventLogIndex: newRegisteringEventLogIndex}),
            ) =>
            // Update DC registration if the new one from the batch has an earlier registration log
            registeringContract.startBlock > dc.startBlock ||
              (registeringContract.startBlock === dc.startBlock &&
                registeringEventLogIndex > newRegisteringEventLogIndex)
          | (Config, _) | (_, Config) =>
            Js.Exn.raiseError(
              "Unexpected case: Config registration should be handled in a different function",
            )
          }
        | None =>
          hasDCWithFilterByAddresses := hasDCWithFilterByAddresses.contents || filterByAddresses
          addressesByContractName->Utils.Dict.push(dc.contractName, dc.address)
          true
        }
        if shouldUpdate {
          earliestRegisteringEventBlockNumber :=
            Pervasives.min(earliestRegisteringEventBlockNumber.contents, dc.startBlock)
          registeringContracts->Js.Dict.set(dc.address->Address.toString, dc)
          let logger = Logging.createChild(
            ~params={
              "chainId": fetchState.chainId,
              "contractAddress": dc.address->Address.toString,
              "contractName": dc.contractName,
              "startBlock": dc.startBlock,
            },
          )
          logger->Logging.childInfo(`Registering dynamic contract with start block: ${dc.startBlock->Int.toString}`)
        }
      }
    | None => {
        let logger = Logging.createChild(
          ~params={
            "chainId": fetchState.chainId,
            "contractAddress": dc.address->Address.toString,
            "contractName": dc.contractName,
          },
        )
        logger->Logging.childWarn(`Skipping contract registration: Contract doesn't have any events to fetch.`)
      }
    }
  }

  let dcsToStore = registeringContracts->Js.Dict.values
  switch dcsToStore {
  // Dont update anything when everything was filter out
  | [] => fetchState
  | _ => {
      let newPartitions = if (
        // This case is more like a simple case when we need to create a single partition.
        // Theoretically, we can only keep else, but don't want to iterate over the addresses again.

        dcsToStore->Array.length <= fetchState.maxAddrInPartition &&
          !hasDCWithFilterByAddresses.contents
      ) {
        // Even in the simple case, we need to respect individual contract start blocks
        // Group by effective start block to create appropriate partitions
        let byEffectiveStartBlock = Js.Dict.empty()
        
        addressesByContractName->Js.Dict.entries->Array.forEach(((contractName, addresses)) => {
          addresses->Array.forEach(address => {
            let indexingContract = registeringContracts->Js.Dict.unsafeGet(address->Address.toString)
            
            // Get the configured start block for this contract type
            let configuredStartBlock = switch contractStartBlocks->Utils.Dict.dangerouslyGetNonOption(contractName) {
            | Some(Some(configStartBlock)) => configStartBlock
            | Some(None) | None => 0 // No configured start block, use 0 as default
            }
            
            // Use the max of configured start block, contract registration block, and earliest registering event block
            let effectiveStartBlock = Pervasives.max(
              configuredStartBlock,
              Pervasives.max(indexingContract.startBlock, earliestRegisteringEventBlockNumber.contents)
            )
            
            // Log the effective start block decision
            if configuredStartBlock > Pervasives.max(indexingContract.startBlock, earliestRegisteringEventBlockNumber.contents) {
              let logger = Logging.createChild(
                ~params={
                  "chainId": fetchState.chainId,
                  "contractAddress": address->Address.toString,
                  "contractName": contractName,
                  "contractStartBlock": indexingContract.startBlock,
                  "configuredStartBlock": configuredStartBlock,
                  "earliestRegisteringBlock": earliestRegisteringEventBlockNumber.contents,
                  "effectiveStartBlock": effectiveStartBlock,
                },
              )
              logger->Logging.childInfo(`Using configured start block ${effectiveStartBlock->Int.toString} for dynamic contract (overrides registration block ${indexingContract.startBlock->Int.toString})`)
            } else if indexingContract.startBlock < earliestRegisteringEventBlockNumber.contents {
              let logger = Logging.createChild(
                ~params={
                  "chainId": fetchState.chainId,
                  "contractAddress": address->Address.toString,
                  "contractName": contractName,
                  "contractStartBlock": indexingContract.startBlock,
                  "earliestRegisteringBlock": earliestRegisteringEventBlockNumber.contents,
                  "effectiveStartBlock": effectiveStartBlock,
                },
              )
              logger->Logging.childWarn(`Dynamic contract start block ${indexingContract.startBlock->Int.toString} is before earliest registering event block ${earliestRegisteringEventBlockNumber.contents->Int.toString}. Using earliest registering event block ${effectiveStartBlock->Int.toString} instead.`)
            } else if indexingContract.startBlock > earliestRegisteringEventBlockNumber.contents {
              let logger = Logging.createChild(
                ~params={
                  "chainId": fetchState.chainId,
                  "contractAddress": address->Address.toString,
                  "contractName": contractName,
                  "contractStartBlock": indexingContract.startBlock,
                  "effectiveStartBlock": effectiveStartBlock,
                },
              )
              logger->Logging.childInfo(`Using contract-specific start block ${effectiveStartBlock->Int.toString} for dynamic contract`)
            }
            
            let effectiveStartBlockKey = effectiveStartBlock->Int.toString
            
            // Group addresses by their effective start blocks
            switch byEffectiveStartBlock->Utils.Dict.dangerouslyGetNonOption(effectiveStartBlockKey) {
            | Some(existingByContractName) => 
              existingByContractName->Utils.Dict.push(contractName, address)
            | None => 
              let newByContractName = Js.Dict.empty()
              newByContractName->Utils.Dict.push(contractName, address)
              byEffectiveStartBlock->Js.Dict.set(effectiveStartBlockKey, newByContractName)
            }
          })
        })
        
        let partitions = []
        byEffectiveStartBlock->Js.Dict.entries->Array.forEach(((effectiveStartBlockKey, addressesByContractNameForBlock)) => {
          let effectiveStartBlockNum = effectiveStartBlockKey->Int.fromString->Option.getExn
          
          partitions->Array.push({
            id: (fetchState.nextPartitionIndex + partitions->Array.length)->Int.toString,
            status: {
              fetchingStateId: None,
            },
            latestFetchedBlock: {
              blockNumber: Pervasives.max(effectiveStartBlockNum - 1, 0),
              blockTimestamp: 0,
            },
            selection: fetchState.normalSelection,
            addressesByContractName: addressesByContractNameForBlock,
          })
        })
        
        partitions
      } else {
        let partitions = []

        // I use for loops instead of forEach, so ReScript better inlines ref access
        for idx in 0 to addressesByContractName->Js.Dict.keys->Array.length - 1 {
          let contractName = addressesByContractName->Js.Dict.keys->Js.Array2.unsafe_get(idx)
          let addresses = addressesByContractName->Js.Dict.unsafeGet(contractName)

          // Can unsafely get it, because we already filtered out the contracts
          // that don't have any events to fetch
          let contractConfig = fetchState.contractConfigs->Js.Dict.unsafeGet(contractName)

          // For this case we can't filter out events earlier than contract registration
          // on the client side, so we need to keep the old logic of creating
          // a partition for every block range, so there are no irrelevant events
          if contractConfig.filterByAddresses {
            let byEffectiveStartBlock = Js.Dict.empty()

            for jdx in 0 to addresses->Array.length - 1 {
              let address = addresses->Js.Array2.unsafe_get(jdx)
              let indexingContract =
                registeringContracts->Js.Dict.unsafeGet(address->Address.toString)

              // Get the configured start block for this contract type
              let configuredStartBlock = switch contractStartBlocks->Utils.Dict.dangerouslyGetNonOption(contractName) {
              | Some(Some(configStartBlock)) => configStartBlock
              | Some(None) | None => 0 // No configured start block, use 0 as default
              }
              
              // Use the max of configured start block, contract registration block, and earliest registering event block
              let effectiveStartBlock = Pervasives.max(
                configuredStartBlock,
                Pervasives.max(indexingContract.startBlock, earliestRegisteringEventBlockNumber.contents)
              )
              
              // Log the effective start block decision
              if configuredStartBlock > Pervasives.max(indexingContract.startBlock, earliestRegisteringEventBlockNumber.contents) {
                let logger = Logging.createChild(
                  ~params={
                    "chainId": fetchState.chainId,
                    "contractAddress": address->Address.toString,
                    "contractName": contractName,
                    "contractStartBlock": indexingContract.startBlock,
                    "configuredStartBlock": configuredStartBlock,
                    "earliestRegisteringBlock": earliestRegisteringEventBlockNumber.contents,
                    "effectiveStartBlock": effectiveStartBlock,
                  },
                )
                logger->Logging.childInfo(`Using configured start block ${effectiveStartBlock->Int.toString} for dynamic contract (overrides registration block ${indexingContract.startBlock->Int.toString})`)
              } else if indexingContract.startBlock < earliestRegisteringEventBlockNumber.contents {
                let logger = Logging.createChild(
                  ~params={
                    "chainId": fetchState.chainId,
                    "contractAddress": address->Address.toString,
                    "contractName": contractName,
                    "contractStartBlock": indexingContract.startBlock,
                    "earliestRegisteringBlock": earliestRegisteringEventBlockNumber.contents,
                    "effectiveStartBlock": effectiveStartBlock,
                  },
                )
                logger->Logging.childWarn(`Dynamic contract start block ${indexingContract.startBlock->Int.toString} is before earliest registering event block ${earliestRegisteringEventBlockNumber.contents->Int.toString}. Using earliest registering event block ${effectiveStartBlock->Int.toString} instead.`)
              } else if indexingContract.startBlock > earliestRegisteringEventBlockNumber.contents {
                let logger = Logging.createChild(
                  ~params={
                    "chainId": fetchState.chainId,
                    "contractAddress": address->Address.toString,
                    "contractName": contractName,
                    "contractStartBlock": indexingContract.startBlock,
                    "effectiveStartBlock": effectiveStartBlock,
                  },
                )
                logger->Logging.childInfo(`Using contract-specific start block ${effectiveStartBlock->Int.toString} for dynamic contract`)
              }

              byEffectiveStartBlock->Utils.Dict.push(effectiveStartBlock->Int.toString, address)
            }

            // Will be in the ASC order by Js spec
            byEffectiveStartBlock
            ->Js.Dict.keys
            ->Js.Array2.forEach(effectiveStartBlockKey => {
              let addresses = byEffectiveStartBlock->Js.Dict.unsafeGet(effectiveStartBlockKey)
              let addressesByContractName = Js.Dict.empty()
              addressesByContractName->Js.Dict.set(contractName, addresses)
              let effectiveStartBlockNum = effectiveStartBlockKey->Int.fromString->Option.getExn
              let logger = Logging.createChild(
                ~params={
                  "chainId": fetchState.chainId,
                  "contractName": contractName,
                  "startBlock": effectiveStartBlockNum,
                  "addressCount": addresses->Array.length,
                },
              )
              logger->Logging.childInfo(`Creating partition for dynamic contracts with effective start block: ${effectiveStartBlockNum->Int.toString}`)
              partitions->Array.push({
                id: (fetchState.nextPartitionIndex + partitions->Array.length)->Int.toString,
                status: {
                  fetchingStateId: None,
                },
                latestFetchedBlock: {
                  blockNumber: Pervasives.max(effectiveStartBlockNum - 1, 0),
                  blockTimestamp: 0,
                },
                selection: fetchState.normalSelection,
                addressesByContractName,
              })
            })
          } else {
            // For contracts that don't filter by addresses, we need to group them by effective start block
            // to ensure proper partitioning based on individual contract start blocks
            let byEffectiveStartBlock = Js.Dict.empty()

            for jdx in 0 to addresses->Array.length - 1 {
              let address = addresses->Js.Array2.unsafe_get(jdx)
              let indexingContract =
                registeringContracts->Js.Dict.unsafeGet(address->Address.toString)

              // Get the configured start block for this contract type
              let configuredStartBlock = switch contractStartBlocks->Utils.Dict.dangerouslyGetNonOption(contractName) {
              | Some(Some(configStartBlock)) => configStartBlock
              | Some(None) | None => 0 // No configured start block, use 0 as default
              }
              
              // Use the max of configured start block, contract registration block, and earliest registering event block
              let effectiveStartBlock = Pervasives.max(
                configuredStartBlock,
                Pervasives.max(indexingContract.startBlock, earliestRegisteringEventBlockNumber.contents)
              )
              
              // Log the effective start block decision
              if configuredStartBlock > Pervasives.max(indexingContract.startBlock, earliestRegisteringEventBlockNumber.contents) {
                let logger = Logging.createChild(
                  ~params={
                    "chainId": fetchState.chainId,
                    "contractAddress": address->Address.toString,
                    "contractName": contractName,
                    "contractStartBlock": indexingContract.startBlock,
                    "configuredStartBlock": configuredStartBlock,
                    "earliestRegisteringBlock": earliestRegisteringEventBlockNumber.contents,
                    "effectiveStartBlock": effectiveStartBlock,
                  },
                )
                logger->Logging.childInfo(`Using configured start block ${effectiveStartBlock->Int.toString} for dynamic contract (overrides registration block ${indexingContract.startBlock->Int.toString})`)
              } else if indexingContract.startBlock < earliestRegisteringEventBlockNumber.contents {
                let logger = Logging.createChild(
                  ~params={
                    "chainId": fetchState.chainId,
                    "contractAddress": address->Address.toString,
                    "contractName": contractName,
                    "contractStartBlock": indexingContract.startBlock,
                    "earliestRegisteringBlock": earliestRegisteringEventBlockNumber.contents,
                    "effectiveStartBlock": effectiveStartBlock,
                  },
                )
                logger->Logging.childWarn(`Dynamic contract start block ${indexingContract.startBlock->Int.toString} is before earliest registering event block ${earliestRegisteringEventBlockNumber.contents->Int.toString}. Using earliest registering event block ${effectiveStartBlock->Int.toString} instead.`)
              } else if indexingContract.startBlock > earliestRegisteringEventBlockNumber.contents {
                let logger = Logging.createChild(
                  ~params={
                    "chainId": fetchState.chainId,
                    "contractAddress": address->Address.toString,
                    "contractName": contractName,
                    "contractStartBlock": indexingContract.startBlock,
                    "effectiveStartBlock": effectiveStartBlock,
                  },
                )
                logger->Logging.childInfo(`Using contract-specific start block ${effectiveStartBlock->Int.toString} for dynamic contract`)
              }

              byEffectiveStartBlock->Utils.Dict.push(effectiveStartBlock->Int.toString, {
                address,
                contractName,
                startBlock: effectiveStartBlock,
                register: indexingContract.register,
              })
            }

            // Process each effective start block group
            byEffectiveStartBlock
            ->Js.Dict.keys
            ->Js.Array2.forEach(effectiveStartBlockKey => {
              let contractsForStartBlock = byEffectiveStartBlock->Js.Dict.unsafeGet(effectiveStartBlockKey)
              let effectiveStartBlockNum = effectiveStartBlockKey->Int.fromString->Option.getExn
              
              // Group by contract name within this start block
              let addressesByContractNameForStartBlock = Js.Dict.empty()
              contractsForStartBlock->Array.forEach(({address, contractName}) => {
                addressesByContractNameForStartBlock->Utils.Dict.push(contractName, address)
              })

              // Split into partitions respecting maxAddrInPartition
              let pendingAddressesByContractName = ref(Js.Dict.empty())
              let pendingCount = ref(0)

              let addPartitionForStartBlock = () =>
                if pendingCount.contents > 0 {
                  let logger = Logging.createChild(
                    ~params={
                      "chainId": fetchState.chainId,
                      "startBlock": effectiveStartBlockNum,
                      "addressCount": pendingCount.contents,
                    },
                  )
                  logger->Logging.childInfo(`Creating partition for dynamic contracts with effective start block: ${effectiveStartBlockNum->Int.toString}`)
                  partitions->Array.push({
                    id: (fetchState.nextPartitionIndex + partitions->Array.length)->Int.toString,
                    status: {
                      fetchingStateId: None,
                    },
                    latestFetchedBlock: {
                      blockNumber: Pervasives.max(effectiveStartBlockNum - 1, 0),
                      blockTimestamp: 0,
                    },
                    selection: fetchState.normalSelection,
                    addressesByContractName: pendingAddressesByContractName.contents,
                  })
                }

              // Add addresses to partitions, creating new partitions when hitting the limit
              addressesByContractNameForStartBlock->Js.Dict.keys->Array.forEach(contractName => {
                let addresses = addressesByContractNameForStartBlock->Js.Dict.unsafeGet(contractName)
                for addressIdx in 0 to addresses->Array.length - 1 {
                  let address = addresses->Js.Array2.unsafe_get(addressIdx)
                  
                  if pendingCount.contents >= fetchState.maxAddrInPartition {
                    addPartitionForStartBlock()
                    pendingAddressesByContractName := Js.Dict.empty()
                    pendingCount := 0
                  }

                  pendingAddressesByContractName.contents->Utils.Dict.push(contractName, address)
                  pendingCount := pendingCount.contents + 1
                }
              })

              // Create final partition for any remaining addresses in this start block group
              addPartitionForStartBlock()
            })
          }
        }

        partitions
      }

      Prometheus.IndexingAddresses.set(
        ~addressesCount=fetchState->numAddresses + dcsToStore->Array.length,
        ~chainId=fetchState.chainId,
      )

      fetchState->updateInternal(
        ~partitions=fetchState.partitions->Js.Array2.concat(newPartitions),
        ~currentBlockHeight,
        ~dcsToStore=switch fetchState.dcsToStore {
        | Some(existingDcs) => Some(Array.concat(existingDcs, dcsToStore))
        | None => Some(dcsToStore)
        },
        ~indexingContracts=// We don't need registeringContracts anymore,
        // so we can safely mixin indexingContracts in it
        // The original indexingContracts won't be mutated
        Utils.Dict.mergeInPlace(registeringContracts, indexingContracts),
        ~nextPartitionIndex=fetchState.nextPartitionIndex + newPartitions->Array.length,
      )
    }
  }
}

type queryTarget =
  | Head
  | EndBlock({toBlock: int})
  | Merge({
      // The partition we are going to merge into
      // It shouldn't be fetching during the query
      intoPartitionId: string,
      toBlock: int,
    })

type query = {
  partitionId: string,
  fromBlock: int,
  selection: selection,
  addressesByContractName: dict<array<Address.t>>,
  target: queryTarget,
  indexingContracts: dict<indexingContract>,
}

exception UnexpectedPartitionNotFound({partitionId: string})
exception UnexpectedMergeQueryResponse({message: string})

/*
Updates fetchState with a response for a given query.
Returns Error if the partition with given query cannot be found (unexpected)
If MergeQuery caught up to the target partition, it triggers the merge of the partitions.

newItems are ordered earliest to latest (as they are returned from the worker)
*/
let handleQueryResult = (
  {partitions} as fetchState: t,
  ~query: query,
  ~latestFetchedBlock: blockNumberAndTimestamp,
  ~reversedNewItems,
  ~currentBlockHeight,
): result<t, exn> =>
  {
    let partitionId = query.partitionId

    switch partitions->Array.getIndexBy(p => p.id === partitionId) {
    | Some(pIndex) =>
      let p = partitions->Js.Array2.unsafe_get(pIndex)
      let updatedPartition = {
        ...p,
        status: {
          fetchingStateId: None,
        },
        latestFetchedBlock,
      }

      switch query.target {
      | Head
      | EndBlock(_) =>
        Ok(partitions->Utils.Array.setIndexImmutable(pIndex, updatedPartition))
      | Merge({intoPartitionId}) =>
        switch partitions->Array.getIndexBy(p => p.id === intoPartitionId) {
        | Some(targetIndex)
          if (partitions->Js.Array2.unsafe_get(targetIndex)).latestFetchedBlock.blockNumber ===
            latestFetchedBlock.blockNumber => {
            let target = partitions->Js.Array2.unsafe_get(targetIndex)
            let (merged, rest) =
              updatedPartition->mergeIntoPartition(
                ~target,
                ~maxAddrInPartition=fetchState.maxAddrInPartition,
              )

            let updatedPartitions = partitions->Utils.Array.setIndexImmutable(targetIndex, merged)
            let updatedPartitions = switch rest {
            | Some(rest) => {
                updatedPartitions->Js.Array2.unsafe_set(pIndex, rest)
                updatedPartitions
              }
            | None => updatedPartitions->Utils.Array.removeAtIndex(pIndex)
            }
            Ok(updatedPartitions)
          }
        | _ => Ok(partitions->Utils.Array.setIndexImmutable(pIndex, updatedPartition))
        }
      }
    | None =>
      Error(
        UnexpectedPartitionNotFound({
          partitionId: partitionId,
        }),
      )
    }
  }->Result.map(partitions => {
    fetchState->updateInternal(
      ~partitions,
      ~currentBlockHeight,
      ~queue=mergeSortedEventList(reversedNewItems, fetchState.queue),
    )
  })

let makePartitionQuery = (p: partition, ~indexingContracts, ~endBlock, ~mergeTarget) => {
  let fromBlock = switch p.latestFetchedBlock.blockNumber {
  | 0 => 0
  | latestFetchedBlockNumber => latestFetchedBlockNumber + 1
  }
  switch (endBlock, mergeTarget) {
  | (Some(endBlock), _) if fromBlock > endBlock => None
  | (_, Some(mergeTarget)) =>
    Some(
      Merge({
        toBlock: mergeTarget.latestFetchedBlock.blockNumber,
        intoPartitionId: mergeTarget.id,
      }),
    )
  | (Some(endBlock), None) => Some(EndBlock({toBlock: endBlock}))
  | (None, None) => Some(Head)
  }->Option.map(target => {
    {
      partitionId: p.id,
      fromBlock,
      target,
      selection: p.selection,
      addressesByContractName: p.addressesByContractName,
      indexingContracts,
    }
  })
}

type nextQuery =
  | ReachedMaxConcurrency
  | WaitingForNewBlock
  | NothingToQuery
  | Ready(array<query>)

let startFetchingQueries = ({partitions}: t, ~queries: array<query>, ~stateId) => {
  queries->Array.forEach(q => {
    switch partitions->Js.Array2.find(p => p.id === q.partitionId) {
    // Shouldn't be mutated to None anymore
    // The status will be immutably set to the initial one when we handle response
    | Some(p) => p.status.fetchingStateId = Some(stateId)
    | None => Js.Exn.raiseError("Unexpected case: Couldn't find partition for the fetching query")
    }
  })
}

let addressesByContractNameCount = (addressesByContractName: dict<array<Address.t>>) => {
  let numAddresses = ref(0)
  let contractNames = addressesByContractName->Js.Dict.keys
  for idx in 0 to contractNames->Array.length - 1 {
    let contractName = contractNames->Js.Array2.unsafe_get(idx)
    numAddresses :=
      numAddresses.contents + addressesByContractName->Js.Dict.unsafeGet(contractName)->Array.length
  }
  numAddresses.contents
}

let addressesByContractNameGetAll = (addressesByContractName: dict<array<Address.t>>) => {
  let all = ref([])
  let contractNames = addressesByContractName->Js.Dict.keys
  for idx in 0 to contractNames->Array.length - 1 {
    let contractName = contractNames->Js.Array2.unsafe_get(idx)
    all := all.contents->Array.concat(addressesByContractName->Js.Dict.unsafeGet(contractName))
  }
  all.contents
}

@inline
let isFullPartition = (p: partition, ~maxAddrInPartition) => {
  switch p {
  | {selection: {dependsOnAddresses: false}} => true
  | _ => p.addressesByContractName->addressesByContractNameCount >= maxAddrInPartition
  }
}

let getNextQuery = (
  {queue, partitions, maxAddrInPartition, endBlock, indexingContracts, blockLag}: t,
  ~concurrencyLimit,
  ~targetBufferSize,
  ~currentBlockHeight,
  ~stateId,
) => {
  if currentBlockHeight === 0 {
    WaitingForNewBlock
  } else if concurrencyLimit === 0 {
    ReachedMaxConcurrency
  } else {
    let headBlock = currentBlockHeight - blockLag->Option.getWithDefault(0)

    let fullPartitions = []
    let mergingPartitions = []
    let areMergingPartitionsFetching = ref(false)
    let mostBehindMergingPartition = ref(None)
    let mergingPartitionTarget = ref(None)
    let shouldWaitForNewBlock = ref(
      switch endBlock {
      | Some(endBlock) => headBlock < endBlock
      | None => true
      },
    )

    let checkIsFetchingPartition = p => {
      switch p.status.fetchingStateId {
      | Some(fetchingStateId) => stateId <= fetchingStateId
      | None => false
      }
    }

    for idx in 0 to partitions->Js.Array2.length - 1 {
      let p = partitions->Js.Array2.unsafe_get(idx)

      let isFetching = checkIsFetchingPartition(p)
      let hasReachedTheHead = p.latestFetchedBlock.blockNumber >= headBlock

      if isFetching || !hasReachedTheHead {
        // Even if there are some partitions waiting for the new block
        // We still want to wait for all partitions reaching the head
        // because they might update currentBlockHeight in their response
        // Also, there are cases when some partitions fetching at 50% of the chain
        // and we don't want to poll the head for a few small partitions
        shouldWaitForNewBlock := false
      }

      if p->isFullPartition(~maxAddrInPartition) {
        fullPartitions->Array.push(p)
      } else {
        mergingPartitions->Array.push(p)

        mostBehindMergingPartition :=
          switch mostBehindMergingPartition.contents {
          | Some(mostBehindMergingPartition) =>
            if (
              // The = check is important here. We don't want to have a target
              // with the same latestFetchedBlock. They should be merged in separate queries
              mostBehindMergingPartition.latestFetchedBlock.blockNumber ===
                p.latestFetchedBlock.blockNumber
            ) {
              mostBehindMergingPartition
            } else if (
              mostBehindMergingPartition.latestFetchedBlock.blockNumber <
              p.latestFetchedBlock.blockNumber
            ) {
              mergingPartitionTarget :=
                switch mergingPartitionTarget.contents {
                | Some(mergingPartitionTarget)
                  if mergingPartitionTarget.latestFetchedBlock.blockNumber <
                  p.latestFetchedBlock.blockNumber => mergingPartitionTarget
                | _ => p
                }->Some
              mostBehindMergingPartition
            } else {
              mergingPartitionTarget := Some(mostBehindMergingPartition)
              p
            }
          | None => p
          }->Some

        if isFetching {
          areMergingPartitionsFetching := true
        }
      }
    }

    // We want to limit the buffer size to targetBufferSize (usually 3 * batchSize)
    // To make sure the processing always has some buffer
    // and not increase the memory usage too much
    // If a partition fetched further than 3 * batchSize,
    // it should be skipped until the buffer is consumed
    let maxQueryBlockNumber = {
      let targetBlockIdx = queue->Array.length - targetBufferSize
      if targetBlockIdx < 0 {
        currentBlockHeight
      } else {
        switch queue->Array.get(targetBlockIdx) {
        | Some(item) => Pervasives.min(item.blockNumber, currentBlockHeight) // Just in case check that we don't query beyond the current block
        | None => currentBlockHeight
        }
      }
    }
    let queries = []

    let registerPartitionQuery = (p, ~mergeTarget=?) => {
      if (
        p->checkIsFetchingPartition->not && p.latestFetchedBlock.blockNumber < maxQueryBlockNumber
      ) {
        switch p->makePartitionQuery(
          ~indexingContracts,
          ~endBlock=switch blockLag {
          | Some(_) =>
            switch endBlock {
            | Some(endBlock) => Some(Pervasives.min(headBlock, endBlock))
            // Force head block as an endBlock when blockLag is set
            // because otherwise HyperSync might return bigger range
            | None => Some(headBlock)
            }
          | None => endBlock
          },
          ~mergeTarget,
        ) {
        | Some(q) => queries->Array.push(q)
        | None => ()
        }
      }
    }

    fullPartitions->Array.forEach(p => p->registerPartitionQuery)

    if areMergingPartitionsFetching.contents->not {
      switch mergingPartitions {
      | [] => ()
      | [p] => p->registerPartitionQuery
      | _ =>
        switch (mostBehindMergingPartition.contents, mergingPartitionTarget.contents) {
        | (Some(p), None) => p->registerPartitionQuery
        | (Some(p), Some(mergeTarget)) => p->registerPartitionQuery(~mergeTarget)
        | (None, _) =>
          Js.Exn.raiseError("Unexpected case, should always have a most behind partition.")
        }
      }
    }

    if queries->Utils.Array.isEmpty {
      if shouldWaitForNewBlock.contents {
        WaitingForNewBlock
      } else {
        NothingToQuery
      }
    } else {
      Ready(
        if queries->Array.length > concurrencyLimit {
          queries
          ->Js.Array2.sortInPlaceWith((a, b) => a.fromBlock - b.fromBlock)
          ->Js.Array2.slice(~start=0, ~end_=concurrencyLimit)
        } else {
          queries
        },
      )
    }
  }
}

type itemWithPopFn = {item: Internal.eventItem, popItemOffQueue: unit => unit}

/**
Represents a fetchState partitions head of the  fetchedEventQueue as either
an existing item, or no item with latest fetched block data
*/
type queueItem =
  | Item(itemWithPopFn)
  | NoItem({latestFetchedBlock: blockNumberAndTimestamp})

let queueItemBlockNumber = (queueItem: queueItem) => {
  switch queueItem {
  | Item({item}) => item.blockNumber
  | NoItem({latestFetchedBlock: {blockNumber}}) => blockNumber === 0 ? 0 : blockNumber + 1
  }
}

let queueItemIsInReorgThreshold = (
  queueItem: queueItem,
  ~currentBlockHeight,
  ~highestBlockBelowThreshold,
) => {
  if currentBlockHeight === 0 {
    false
  } else {
    queueItem->queueItemBlockNumber > highestBlockBelowThreshold
  }
}

/**
Simple constructor for no item from partition
*/
let makeNoItem = ({latestFetchedBlock}: partition) => NoItem({
  latestFetchedBlock: latestFetchedBlock,
})

/**
Creates a compareable value for items and no items on partition queues.
Block number takes priority here. Since a latest fetched timestamp could
be zero from initialization of partition but a higher latest fetched block number exists

Note: on the chain manager, when comparing multi chain, the timestamp is the highest priority compare value
*/
let qItemLt = (a, b) => {
  let aBlockNumber = a->queueItemBlockNumber
  let bBlockNumber = b->queueItemBlockNumber
  if aBlockNumber < bBlockNumber {
    true
  } else if aBlockNumber === bBlockNumber {
    switch (a, b) {
    | (Item(a), Item(b)) => a.item.logIndex < b.item.logIndex
    | (NoItem(_), Item(_)) => true
    | (Item(_), NoItem(_))
    | (NoItem(_), NoItem(_)) => false
    }
  } else {
    false
  }
}

/**
Gets the earliest queueItem from thgetNodeEarliestEventWithUpdatedQueue.

Finds the earliest queue item across all partitions and then returns that
queue item with an update fetch state.
*/
let getEarliestEvent = ({queue, latestFullyFetchedBlock}: t) => {
  switch queue->Utils.Array.last {
  | Some(item) =>
    if item.blockNumber <= latestFullyFetchedBlock.blockNumber {
      Item({item, popItemOffQueue: () => queue->Js.Array2.pop->ignore})
    } else {
      NoItem({
        latestFetchedBlock: latestFullyFetchedBlock,
      })
    }
  | None =>
    NoItem({
      latestFetchedBlock: latestFullyFetchedBlock,
    })
  }
}

/**
Instantiates a fetch state with partitions for initial addresses
*/
let make = (
  ~startBlock,
  ~endBlock,
  ~eventConfigs: array<Internal.eventConfig>,
  ~staticContracts: dict<array<Address.t>>,
  ~staticContractsWithStartBlocks: dict<option<int>>,
  ~dynamicContracts: array<indexingContract>,
  ~maxAddrInPartition,
  ~chainId,
  ~blockLag=?,
): t => {
  // Compress static and dynamic contracts into a single array<IndexingContract>
  let allIndexingContracts = []
  
  // Add static contracts to the combined array
  staticContracts->Js.Dict.entries->Array.forEach(((contractName, addresses)) => {
    let contractStartBlock = switch staticContractsWithStartBlocks->Utils.Dict.dangerouslyGetNonOption(contractName) {
    | Some(Some(contractStartBlock)) => 
      let logger = Logging.createChild(~params={"chainId": chainId, "contractName": contractName})
      logger->Logging.childInfo(`Using contract-specific start block: ${contractStartBlock->Int.toString}`)
      contractStartBlock
    | Some(None) => 
      let logger = Logging.createChild(~params={"chainId": chainId, "contractName": contractName})
      logger->Logging.childInfo(`Contract start block is None, using network start block: ${startBlock->Int.toString}`)
      startBlock
    | None => 
      let logger = Logging.createChild(~params={"chainId": chainId, "contractName": contractName})
      logger->Logging.childInfo(`No contract start block found, using network start block: ${startBlock->Int.toString}`)
      startBlock
    }
    
    addresses->Array.forEach(address => {
      let logger = Logging.createChild(~params={"chainId": chainId, "contractName": contractName, "address": address->Address.toString})
      logger->Logging.childInfo(`Creating indexing contract with start block: ${contractStartBlock->Int.toString}`)
      allIndexingContracts->Array.push({
        address,
        contractName,
        startBlock: contractStartBlock,
        register: Config,
      })
    })
  })
  
  // Add dynamic contracts to the combined array
  dynamicContracts->Array.forEach(dc => {
    allIndexingContracts->Array.push(dc)
  })

  // We no longer use a global earliest start block for all partitions
  // Instead, we'll create partitions with their own latestFetchedBlock based on contract start blocks

  let notDependingOnAddresses = []
  let normalEventConfigs = []
  let contractNamesWithNormalEvents = Utils.Set.make()
  let indexingContracts = Js.Dict.empty()
  let contractConfigs = Js.Dict.empty()

  eventConfigs->Array.forEach(ec => {
    switch contractConfigs->Utils.Dict.dangerouslyGetNonOption(ec.contractName) {
    | Some({filterByAddresses}) =>
      contractConfigs->Js.Dict.set(
        ec.contractName,
        {filterByAddresses: filterByAddresses || ec.filterByAddresses},
      )
    | None =>
      contractConfigs->Js.Dict.set(ec.contractName, {filterByAddresses: ec.filterByAddresses})
    }

    if ec.dependsOnAddresses {
      normalEventConfigs->Array.push(ec)
      contractNamesWithNormalEvents->Utils.Set.add(ec.contractName)->ignore
    } else {
      notDependingOnAddresses->Array.push(ec)
    }
  })

  let partitions = []

  // For events that don't depend on addresses, use the network start block
  if notDependingOnAddresses->Array.length > 0 {
    let latestFetchedBlockForNonAddressDependentEvents = {
      blockTimestamp: 0,
      blockNumber: startBlock - 1,
    }
    partitions->Array.push({
      id: partitions->Array.length->Int.toString,
      status: {
        fetchingStateId: None,
      },
      latestFetchedBlock: latestFetchedBlockForNonAddressDependentEvents,
      selection: {
        dependsOnAddresses: false,
        eventConfigs: notDependingOnAddresses,
      },
      addressesByContractName: Js.Dict.empty(),
    })
  }

  let normalSelection = {
    dependsOnAddresses: true,
    eventConfigs: normalEventConfigs,
  }

  switch normalEventConfigs {
  | [] => ()
  | _ => {
      // Group contracts by their start blocks to create efficient partitions
      let contractsByStartBlock = Js.Dict.empty()
      
      allIndexingContracts->Array.forEach(contract => {
        if contractNamesWithNormalEvents->Utils.Set.has(contract.contractName) {
          let startBlockKey = contract.startBlock->Int.toString
          contractsByStartBlock->Utils.Dict.push(startBlockKey, contract)
          indexingContracts->Js.Dict.set(contract.address->Address.toString, contract)
        }
      })

      // Create partitions for each unique start block
      contractsByStartBlock->Js.Dict.entries->Array.forEach(((startBlockKey, contractsForStartBlock)) => {
        let startBlockNum = startBlockKey->Int.fromString->Option.getExn
        let latestFetchedBlockForThisStartBlock = {
          blockTimestamp: 0,
          blockNumber: Pervasives.max(startBlockNum - 1, 0),
        }

        // Group contracts by contract name within this start block
        let addressesByContractName = Js.Dict.empty()
        contractsForStartBlock->Array.forEach(contract => {
          addressesByContractName->Utils.Dict.push(contract.contractName, contract.address)
        })

        // Create partitions respecting maxAddrInPartition limit
        let contractNames = addressesByContractName->Js.Dict.keys
        let pendingAddressesByContractName = ref(Js.Dict.empty())
        let pendingAddressCount = ref(0)

        let createPartition = () => {
          if pendingAddressCount.contents > 0 {
            partitions->Array.push({
              id: partitions->Array.length->Int.toString,
              status: {
                fetchingStateId: None,
              },
              latestFetchedBlock: latestFetchedBlockForThisStartBlock,
              selection: normalSelection,
              addressesByContractName: pendingAddressesByContractName.contents,
            })
          }
        }

        for contractNameIdx in 0 to contractNames->Array.length - 1 {
          let contractName = contractNames->Js.Array2.unsafe_get(contractNameIdx)
          let addresses = addressesByContractName->Js.Dict.unsafeGet(contractName)
          
          for addressIdx in 0 to addresses->Array.length - 1 {
            let address = addresses->Js.Array2.unsafe_get(addressIdx)
            
            // Check if adding this address would exceed the limit
            if pendingAddressCount.contents >= maxAddrInPartition {
              createPartition()
              // Reset for next partition
              pendingAddressesByContractName := Js.Dict.empty()
              pendingAddressCount := 0
            }
            
            // Add address to pending partition
            pendingAddressesByContractName.contents->Utils.Dict.push(contractName, address)
            pendingAddressCount := pendingAddressCount.contents + 1
          }
        }

        // Create final partition for any remaining addresses
        createPartition()
      })
    }
  }

  if partitions->Array.length === 0 {
    Js.Exn.raiseError(
      "Invalid configuration: Nothing to fetch. Make sure that you provided at least one contract address to index, or have events with Wildcard mode enabled.",
    )
  }

  let numAddresses = indexingContracts->Js.Dict.keys->Array.length
  
  // Compute the minimum latestFetchedBlock from all partitions
  let latestFullyFetchedBlock = switch partitions->Array.get(0) {
  | Some(firstPartition) => {
      let minBlock = ref(firstPartition.latestFetchedBlock)
      for idx in 1 to partitions->Array.length - 1 {
        let p = partitions->Js.Array2.unsafe_get(idx)
        if p.latestFetchedBlock.blockNumber < minBlock.contents.blockNumber {
          minBlock := p.latestFetchedBlock
        }
      }
      minBlock.contents
    }
  | None => {
      // Fallback if no partitions (shouldn't happen due to validation above)
      {
        blockTimestamp: 0,
        blockNumber: startBlock - 1,
      }
    }
  }
  
  Prometheus.IndexingAddresses.set(~addressesCount=numAddresses, ~chainId)
  Prometheus.IndexingPartitions.set(~partitionsCount=partitions->Array.length, ~chainId)
  Prometheus.IndexingBufferSize.set(~bufferSize=0, ~chainId)
  Prometheus.IndexingBufferBlockNumber.set(~blockNumber=latestFullyFetchedBlock.blockNumber, ~chainId)
  switch endBlock {
  | Some(endBlock) => Prometheus.IndexingEndBlock.set(~endBlock, ~chainId)
  | None => ()
  }

  {
    partitions,
    nextPartitionIndex: partitions->Array.length,
    contractConfigs,
    isFetchingAtHead: false,
    maxAddrInPartition,
    chainId,
    endBlock,
    latestFullyFetchedBlock,
    firstEventBlockNumber: None,
    normalSelection,
    indexingContracts,
    dcsToStore: None,
    blockLag,
    queue: [],
  }
}

let queueSize = ({queue}: t) => queue->Array.length

/**
* Returns the latest block number fetched for the lowest fetcher queue (ie the earliest un-fetched dynamic contract)
*/
let getLatestFullyFetchedBlock = ({latestFullyFetchedBlock}: t) => latestFullyFetchedBlock

let pruneQueueFromFirstChangeEvent = (
  queue: array<Internal.eventItem>,
  ~firstChangeEvent: blockNumberAndLogIndex,
) => {
  queue->Array.keep(item =>
    (item.blockNumber, item.logIndex) < (firstChangeEvent.blockNumber, firstChangeEvent.logIndex)
  )
}

/**
Rolls back partitions to the given valid block
*/
let rollbackPartition = (
  p: partition,
  ~firstChangeEvent: blockNumberAndLogIndex,
  ~addressesToRemove,
) => {
  switch p {
  | {selection: {dependsOnAddresses: false}} =>
    Some({
      ...p,
      status: {
        fetchingStateId: None,
      },
    })
  | {addressesByContractName} =>
    let rollbackedAddressesByContractName = Js.Dict.empty()
    addressesByContractName->Utils.Dict.forEachWithKey((contractName, addresses) => {
      let keptAddresses =
        addresses->Array.keep(address => !(addressesToRemove->Utils.Set.has(address)))
      if keptAddresses->Array.length > 0 {
        rollbackedAddressesByContractName->Js.Dict.set(contractName, keptAddresses)
      }
    })

    if rollbackedAddressesByContractName->Js.Dict.keys->Array.length === 0 {
      None
    } else {
      let shouldRollbackFetched = p.latestFetchedBlock.blockNumber >= firstChangeEvent.blockNumber

      Some({
        id: p.id,
        selection: p.selection,
        status: {
          fetchingStateId: None,
        },
        addressesByContractName: rollbackedAddressesByContractName,
        latestFetchedBlock: shouldRollbackFetched
          ? {
              blockNumber: firstChangeEvent.blockNumber - 1,
              blockTimestamp: 0,
            }
          : p.latestFetchedBlock,
      })
    }
  }
}

let rollback = (fetchState: t, ~firstChangeEvent) => {
  let addressesToRemove = Utils.Set.make()
  let indexingContracts = Js.Dict.empty()

  fetchState.indexingContracts
  ->Js.Dict.keys
  ->Array.forEach(address => {
    let indexingContract = fetchState.indexingContracts->Js.Dict.unsafeGet(address)
    if (
      switch indexingContract {
      | {register: Config} => true
      | {register: DC(dc)} =>
        indexingContract.startBlock < firstChangeEvent.blockNumber ||
          (indexingContract.startBlock === firstChangeEvent.blockNumber &&
            dc.registeringEventLogIndex < firstChangeEvent.logIndex)
      }
    ) {
      indexingContracts->Js.Dict.set(address, indexingContract)
    } else {
      //If the registration block is later than the first change event,
      //Do not keep it and add to the removed addresses
      let _ = addressesToRemove->Utils.Set.add(address->Address.unsafeFromString)
    }
  })

  let partitions =
    fetchState.partitions->Array.keepMap(p =>
      p->rollbackPartition(~firstChangeEvent, ~addressesToRemove)
    )

  fetchState->updateInternal(
    ~partitions,
    ~indexingContracts,
    ~queue=fetchState.queue->pruneQueueFromFirstChangeEvent(~firstChangeEvent),
    ~dcsToStore=switch fetchState.dcsToStore {
    | Some(dcsToStore) =>
      let filtered =
        dcsToStore->Js.Array2.filter(dc => !(addressesToRemove->Utils.Set.has(dc.address)))
      switch filtered {
      | [] => None
      | _ => Some(filtered)
      }
    | None => None
    },
  )
}

/**
* Returns a boolean indicating whether the fetch state is actively indexing
* used for comparing event queues in the chain manager
*/
let isActivelyIndexing = ({latestFullyFetchedBlock, endBlock} as fetchState: t) => {
  switch endBlock {
  | Some(endBlock) =>
    let isPastEndblock = latestFullyFetchedBlock.blockNumber >= endBlock
    if isPastEndblock {
      fetchState->queueSize > 0
    } else {
      true
    }
  | None => true
  }
}
