import {
  Pipeline,
  PipelineEventEmitter,
  PipelineFunctionMap,
  PipelineInput,
  PipelineModuleMap,
  PipelineRunConfig,
  PipelineState,
} from "./pipeline";

export class Workflows {
  constructor(
    public readonly pipelines: Pipeline[],
    private modules: PipelineModuleMap = {},
  ) {
    this.pipelines
      .filter(p => p.path && p.entrypoint)
      .forEach(event => {
        this.events.on(event.functionName, (trigger) => {
          if (event.path == trigger.workflow) {
            this.run(event.workflow, event.name, { event: event.path }, event.reset().copy(trigger))
          }
        })
      })
  }

  public readonly events: PipelineEventEmitter = new PipelineEventEmitter()
  
  private functions: PipelineFunctionMap = {}

  static fromJson(json: any[]) {
    const pipelines = json.map((pipeline) => Pipeline.fromJson(pipeline));
    return new Workflows(pipelines);
  }

  bindModules(modules: PipelineModuleMap) {
    this.modules = modules
  }

  async run(workflow: string, pipeline: string, config: PipelineRunConfig = {}, initialPipelineState?: Pipeline) {
    const pipelines: { [pipeline: string]: Pipeline } = this.pipelines
      .filter(p => p.workflow == workflow)
      .reduce((acc, p) => ({ ...acc, [p.name]: p.reset() }), {})
    var nextPipeline = pipelines[pipeline]
    
    if (!nextPipeline) {
      const functionName = pipeline.split(':')[0]
      nextPipeline = new Pipeline(pipeline, functionName, null, workflow, [], [], [], false, [], [])
      pipelines[pipeline] = nextPipeline
    }
    if (config.args) {
      nextPipeline.args = config.args
    }
    if (nextPipeline && initialPipelineState) {
      nextPipeline.copy(initialPipelineState)
    }
    if (!nextPipeline.entrypoint) {
      nextPipeline.fanIn = []
      nextPipeline.fanOut = []
      nextPipeline.executionPlan = [nextPipeline.name]
    }
    if (nextPipeline.entrypoint || nextPipeline.executionPlan.length > 1) {
      console.log(`🟢 Running workflow '${nextPipeline.workflow}' execution plan:`)
      for (var i = 0; i < nextPipeline.executionPlan.length; i++) {
        const entrypoint = nextPipeline.entrypoint && nextPipeline.name == nextPipeline.executionPlan[i]
        console.log(`  └─ ${i + 1}. ${nextPipeline.executionPlan[i]} ${entrypoint ? '▶️' : ''}`);
      }
      console.log('')
    }
    
    await this.runExecutionPlan(pipelines, nextPipeline.executionPlan, config)
    
    if (!pipelines['output']) return null
    return pipelines['output'].input
  }

  private async runExecutionPlan(pipelines: Record<string, Pipeline>, executionPlan: string[], config: PipelineRunConfig) {
    for (var executionIndex = 0; executionIndex < executionPlan.length; executionIndex++) {
      try {
        const pipeline = pipelines[executionPlan[executionIndex]]
        if (!pipeline) {
          break
        }
        if (pipeline.fanIn.some(fanIn => !pipelines[fanIn].output)) {
          continue
        }
        if (pipeline.state == PipelineState.SKIP) {
          continue
        }
        
        if (pipeline.input.length == 0) {
          pipeline.input = this.getPipelineInput(pipelines, pipeline)
        }
        if (pipeline.input.length >= 2) {
          executionPlan.splice(executionIndex, 0, ...pipeline.executionPlan)
        }

        var inputWithArgs: PipelineInput = {}
        inputWithArgs = pipeline.input.shift()!
        inputWithArgs = { ...inputWithArgs, ...pipeline.args }
        
        if (pipeline.path && pipeline.path != config.event) {
          await this.runPipelineFromPath(pipelines, pipeline, inputWithArgs)
          continue
        }
        
        this.emitPipelineState(pipeline, PipelineState.EXEC, config, inputWithArgs)
        
        await this.executePipeline(pipeline, inputWithArgs, config)
  
        this.emitPipelineState(pipeline, PipelineState.DONE, config)
      } catch (e: any) {
        const pipeline = pipelines[executionPlan[executionIndex]]
        if (pipeline) {
          pipeline.output = null;
          pipeline.error = e.toString();
          this.emitPipelineState(pipeline, PipelineState.FAILED, config)
        }
      }
    }
  }

  private async runPipelineFromPath(pipelines: Record<string, Pipeline>, pipeline: Pipeline, inputWithArgs: PipelineInput) {
    const pipelineRunConfig: PipelineRunConfig = {
      id: Date.now(),
      args: inputWithArgs,
      ignoreEntrypoint: true,
    }
    const path = pipeline.path!
    const name = pipeline.name.split('.')[1]
    const fanOut = pipeline.fanOut
      .filter(fanOut => fanOut.startsWith(`${path}.`))
      .map(fanOut => fanOut.substring(path.length + 1))
    const pipelineRunUnsubscribe = this.events.on(null, (p, c) => {
      if (c?.id != pipelineRunConfig.id) return
      if (p.path) return
      if (p.name == name || fanOut.includes(p.name)) {
        pipelines[`${path}.${p.name}`].copy(p)
        pipelines[`${path}.${p.name}`].state = PipelineState.SKIP
      }
    })
    await this.run(path, name, pipelineRunConfig)
    pipelineRunUnsubscribe()
  }

  private getPipelineInput(pipelines: Record<string, Pipeline>, pipeline: Pipeline) {
    var fanIn = [...pipeline.fanIn]
    if (pipeline.name == 'output') {
      fanIn = Object
        .values(pipelines)
        .filter(pipeline => pipeline.fanOut.includes('output'))
        .map(pipeline => pipeline.name)
    }
    return fanIn
      .filter(fanIn => pipelines[fanIn].output?.length)
      .map(fanIn => pipelines[fanIn].output!)
      // cartesian product
      .reduce<PipelineInput[][]>(
        (acc, curr) => acc.flatMap(a => curr.map(c => [...a, c])),
        [[]])
      // merge combinations
      .map<PipelineInput>(input => Object.assign({}, ...input));
  }

  private emitPipelineState(pipeline: Pipeline, state: PipelineState, config: PipelineRunConfig, inputWithArgs: PipelineInput = {}) {
    pipeline.state = state
    if (state == PipelineState.EXEC) {
      console.log(`▶️  ${pipeline}`)
      for (const [key, value] of Object.entries(inputWithArgs)) {
        console.log(`  └─ ${key}: ${value}`);
      }
    } else if (state == PipelineState.DONE) {
      if (!pipeline.output) {
        console.log(`🛑 ${pipeline}`);
      } else if (pipeline.output!.length >= 2) {
        console.log(`🔁 ${pipeline} (${pipeline.output!.length})`);
      } else {
        console.log(`✅ ${pipeline}`);
      }
      for (const output of pipeline.output ?? []) {
        if (typeof output === "object") {
          for (const [key, value] of Object.entries(output)) {
            console.log(`  └─ ${key}: ${value}`);
          }
        }
      }
    } else if (state == PipelineState.FAILED) {
      console.log(`⚠️  ${pipeline} ${pipeline.error}`);
    }
    console.log('')
    this.events.emit(pipeline, config)
  }

  private async executePipeline(pipeline: Pipeline, inputWithArgs: PipelineInput, config: PipelineRunConfig) {
    const pipelineFunction = pipeline.name == "output"
      ? () => null
      : await this.loadPipelineFunction(pipeline)
    const output = await pipelineFunction(inputWithArgs, config.scope, config.global);
    if (!output) {
      pipeline.output = null;
    } else if (Array.isArray(output)) {
      pipeline.output = output;
    } else if (typeof output === "object" && Object.getPrototypeOf(output) == Object.prototype) {
      pipeline.output = [output];
    } else if (output === true) {
      pipeline.output = [inputWithArgs];
    } else {
      throw new Error(`Unexpected output '${output}' (${typeof output}) for pipeline ${pipeline}.`);
    }
  }

  private async loadPipelineFunction(pipeline: Pipeline) {
    const moduleName = pipeline.path ?? pipeline.workflow
    const functionKey = `${moduleName}.${pipeline.functionName}`
    if (!this.functions[functionKey]) {
      const importedFunctions = await this.modules[moduleName]()
      for (var functionName in importedFunctions) {
        this.functions[`${moduleName}.${functionName}`] = importedFunctions[functionName] 
      }
    }
    if (!this.functions[functionKey]) {
      throw Error(`The function with name '${pipeline.functionName}' was not found in module '${moduleName}'.`)
    }
    return this.functions[functionKey]
  }
}