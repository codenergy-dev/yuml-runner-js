import {
  Pipeline,
  PipelineEventEmitter,
  PipelineFunction,
  PipelineFunctionMap,
  PipelineModuleMap,
  PipelineRunConfig,
  PipelineState,
} from "./pipeline";

export class Workflows {
  constructor(
    public readonly pipelines: Pipeline[],
    private modules: PipelineModuleMap = {},
  ) {}

  public readonly events: PipelineEventEmitter = new PipelineEventEmitter()
  
  private functions: PipelineFunctionMap = {}

  static fromJson(json: any[]) {
    const pipelines = json.map((pipeline) => Pipeline.fromJson(pipeline));
    return new Workflows(pipelines);
  }

  bindModules(modules: PipelineModuleMap) {
    this.modules = modules
  }

  async run(pipeline: Pipeline | PipelineFunction, config?: PipelineRunConfig) {
    const pipelines = [...this.pipelines.map(p => p.reset())]
    const nextPipeline = pipelines
      .find(p => p.entrypoint && p.name == pipeline.name)
    if (nextPipeline && config?.args) {
      nextPipeline.args = config.args
    }
    await this.runNextPipeline(pipelines, nextPipeline, config)
    return pipelines.filter(p => p.state == PipelineState.DONE)
  }

  private async runNextPipeline(pipelines: Pipeline[], pipeline?: Pipeline, config?: PipelineRunConfig): Promise<void> {
    if (!pipeline) return;

    if (pipeline.path) {
      return await this.runNextPipelineFromPath(pipelines, pipeline, config)
    }

    const fanInSet = new Set(pipeline.fanIn);
    const fanInCheckSet = new Set(pipeline.fanInCheck);

    const isReady =
      fanInSet.size === fanInCheckSet.size &&
      [...fanInSet].every(value => fanInCheckSet.has(value));

    if (isReady) {
      if ([PipelineState.IDLE, PipelineState.WAIT].includes(pipeline.state)) {
        pipeline.state = PipelineState.EXEC;
        this.events.emit(pipeline)
        
        const pipelineFunction = await this.loadPipelineFunction(pipeline)
        pipeline.output = await pipelineFunction({ ...pipeline.input, ...pipeline.args }, config?.scope, config?.global);

        pipeline.state = PipelineState.DONE;
        pipeline.fanInCheck = [];
        this.events.emit(pipeline)

        if (!pipeline.output) return;

        if (Array.isArray(pipeline.output)) {
          console.log(`\n🔁 ${pipeline.name} (${pipeline.output.length})`);
        } else if (typeof pipeline.output === "object") {
          console.log(`\n✅ ${pipeline.name}`);
          pipeline.output = [pipeline.output];
        } else {
          throw new Error(`Unexpected output (${pipeline.output}) for pipeline ${pipeline.name}.`);
        }

        for (const output of pipeline.output) {
          if (typeof output === "object") {
            for (const [key, value] of Object.entries(output)) {
              console.log(`  └─ ${key}: ${value}`);
            }
          }
        }
      }

      const fanOutList = pipeline.fanOutPending ? [pipeline.fanOutPending] : pipeline.fanOut;
      pipeline.fanOutPending = null;

      for (const output of pipeline.output!) {
        for (const fanOut of fanOutList) {
          const nextPipeline = pipelines.find(
            p => p.name === fanOut
              && p.workflow == pipeline.workflow
          );
          if (!nextPipeline) continue;

          nextPipeline.state = PipelineState.IDLE;
          nextPipeline.fanInCheck.push(pipeline.name);
          nextPipeline.input = { ...nextPipeline.input, ...output };
        }

        for (const fanOut of fanOutList) {
          const nextPipeline = pipelines.find(
            p => p.name === fanOut
              && [PipelineState.IDLE, PipelineState.WAIT].includes(p.state)
              && p.workflow == pipeline.workflow
          );
          if (nextPipeline) {
            console.log(`\nℹ️  [${pipeline.name}]->[${nextPipeline.name}]`);
          }
          await this.runNextPipeline(pipelines, nextPipeline, config);
        }
      }
    } else {
      pipeline.state = PipelineState.WAIT;
      this.events.emit(pipeline)

      const nextPipeline = pipelines.find(
        p => pipeline.fanIn.includes(p.name)
          && !pipeline.fanInCheck.includes(p.name)
          && p.workflow == pipeline.workflow
      );

      if (nextPipeline) {
        console.log(`\nℹ️  [${nextPipeline.name}]<-[${pipeline.name}]`);
        nextPipeline.fanOutPending = pipeline.name;
      }

      await this.runNextPipeline(pipelines, nextPipeline, config);
    }
  }

  private async runNextPipelineFromPath(pipelines: Pipeline[], pipeline: Pipeline, config?: PipelineRunConfig): Promise<void> {
    var nextPipeline = pipelines
      .find(p => p.workflow == pipeline?.path && p.entrypoint)
    if (nextPipeline) {
      const nextPipelines = await this.run(nextPipeline, config)
      nextPipeline = nextPipelines
        .find(p => p.workflow == pipeline?.path && p.entrypoint)
      if (nextPipeline) {
        pipeline.copyState(nextPipeline)
      }
      
      const nextFanOut = nextPipelines
        .filter(p => pipeline.fanOut.includes(p.name))
      for (var fanOut of nextFanOut) {
        var nextPipelineFanOut = pipelines
          .find(p => pipeline.fanOut.includes(p.name)
                  && p.workflow == pipeline.workflow)
        if (nextPipelineFanOut) {
          nextPipelineFanOut.copyState(fanOut)
          await this.runNextPipeline(pipelines, nextPipelineFanOut, config)
        }
      }
    }
  }

  private async loadPipelineFunction(pipeline: Pipeline) {
    if (!this.functions[pipeline.functionName]) {
      const functions = await this.modules[pipeline.workflow]()
      this.functions = { ...this.functions, ...functions }
    }
    return this.functions[pipeline.functionName]
  }
}