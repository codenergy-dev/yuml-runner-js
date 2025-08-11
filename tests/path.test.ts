import { PipelineState } from '../src/pipeline'
import { Workflows } from '../src/workflows'
import { a } from './pipelines/path'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('path', () => {
  it('validate pipeline call to another workflow', async () => {
    const workflows = Workflows.fromJson([
      ...readWorkflowJson('a-b.json'),
      ...readWorkflowJson('path.json'),
    ])
    workflows.bindModules({
      'a-b': () => import('./pipelines/a-b'),
      'path': () => import('./pipelines/path')
    })

    const history: string[] = []
    workflows.events.on(null, (pipeline) => {
      if (pipeline.workflow == 'a-b') {
        history.push(pipeline.name)
      }
    })
    
    const pipelines = await workflows.runFromWorkflow('path', a)
    expect(pipelines.length).toBe(4)
    expect(pipelines.find(p => p.path == 'a-b')?.state).toBe(PipelineState.DONE)
    expect(history.join('->')).toBe('a->b')
  })
})