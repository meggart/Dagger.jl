# Remote
function eager_submit_internal!(@nospecialize(payload))
    ctx = Dagger.Sch.eager_context()
    state = Dagger.Sch.EAGER_STATE[]
    task = current_task()
    tid = 0
    return eager_submit_internal!(ctx, state, task, tid, payload)
end
function eager_submit_internal!(ctx, state, task, tid, payload; uid_to_tid=Dict{UInt64,Int}())
    @nospecialize payload
    ntasks, uid, future, ref, f, args, options, reschedule = payload

    if uid isa Vector
        thunk_ids = Sch.ThunkID[]
        for i in 1:ntasks
            tid = eager_submit_internal!(ctx, state, task, tid,
                                         (1, uid[i], future[i], ref[i],
                                          f[i], args[i], options[i],
                                          false); uid_to_tid)
            push!(thunk_ids, tid)
            uid_to_tid[uid[i]] = tid.id
        end
        put!(state.chan, Sch.RescheduleSignal())
        return thunk_ids
    end

    timespan_start(ctx, :add_thunk, tid, 0)

    # Lookup EagerThunk/ThunkID -> Thunk
    args::Vector{Any}
    syncdeps = if haskey(options, :syncdeps)
        collect(options.syncdeps)
    else
        nothing
    end::Union{Vector{Any},Nothing}
    lock(Sch.EAGER_ID_MAP) do id_map
        for (idx, (pos, arg)) in enumerate(args)
            pos::Union{Symbol,Nothing}
            newarg = if arg isa EagerThunk
                uid = arg.uid
                tid = if haskey(id_map, uid)
                    id_map[uid]
                else
                    uid_to_tid[uid]
                end
                state.thunk_dict[tid]
            elseif arg isa Sch.ThunkID
                tid = arg.id
                state.thunk_dict[tid]
            else
                arg
            end
            @inbounds args[idx] = pos => newarg
        end
        if syncdeps === nothing
            return
        end
        for (idx, dep) in enumerate(syncdeps)
            newdep = if dep isa EagerThunk
                tid = if haskey(id_map, dep.uid)
                    id_map[dep.uid]
                else
                    uid_to_tid[dep.uid]
                end
                state.thunk_dict[tid]
            elseif dep isa Sch.ThunkID
                tid = dep.id
                state.thunk_dict[tid]
            else
                dep
            end
            @inbounds syncdeps[idx] = newdep
        end
    end
    if syncdeps !== nothing
        options = merge(options, (;syncdeps))
    end

    GC.@preserve args begin
        # Create the `Thunk`
        thunk = Thunk(f, args...; options...)

        # Create a `DRef` to `thunk` so that the caller can preserve it
        thunk_ref = poolset(thunk; size=64, device=MemPool.CPURAMDevice())
        thunk_id = Sch.ThunkID(thunk.id, thunk_ref)

        # Attach `thunk` within the scheduler
        state.thunk_dict[thunk.id] = WeakThunk(thunk)
        Sch.reschedule_syncdeps!(state, thunk)
        @dagdebug thunk :submit "Added to scheduler"
        if future !== nothing
            # Ensure we attach a future before the thunk is scheduled
            Sch._register_future!(ctx, state, task, tid, (future, thunk_id, false))
            @dagdebug thunk :submit "Registered future"
        end
        if ref !== nothing
            # Preserve the `EagerThunkFinalizer` through `thunk`
            thunk.eager_ref = ref
        end
        state.valid[thunk] = nothing

        # Register Eager UID -> Sch TID
        lock(Sch.EAGER_ID_MAP) do id_map
            id_map[uid] = thunk.id
        end

        # Tell the scheduler that it has new tasks to schedule
        if reschedule
            put!(state.chan, Sch.RescheduleSignal())
        end

        timespan_finish(ctx, :add_thunk, tid, 0)

        return thunk_id
    end
end

# Local -> Remote
function eager_submit!(ntasks, uid, future, finalizer_ref, f, args, options)
    if Dagger.in_thunk()
        h = Dagger.sch_handle()
        return exec!(eager_submit_internal!, h, ntasks, uid, future, finalizer_ref, f, args, options, true)
    elseif myid() != 1
        return remotecall_fetch(eager_submit_internal!, 1, (ntasks, uid, future, finalizer_ref, f, args, options, true))
    else
        state = Dagger.Sch.EAGER_STATE[]
        return lock(state.lock) do
            eager_submit_internal!((ntasks, uid, future, finalizer_ref,
                                    f, args, options,
                                    true))
        end
    end
end

# Submission -> Local
function eager_process_elem_submission_to_local(id_map, x)
    @nospecialize x
    @assert !isa(x, Thunk) "Cannot use `Thunk`s in `@spawn`/`spawn`"
    if x isa Dagger.EagerThunk && haskey(id_map, x.uid)
        return Sch.ThunkID(id_map[x.uid], x.thunk_ref)
    elseif x isa Dagger.Chunk
        return WeakChunk(x)
    else
        return x
    end
end
# TODO: This can probably operate in-place
function eager_process_args_submission_to_local(id_map, spec::Pair{EagerTaskSpec,EagerThunk})
    return Base.mapany(first(spec).args) do pos_x
        pos, x = pos_x
        return pos => eager_process_elem_submission_to_local(id_map, x)
    end
end
function eager_process_args_submission_to_local(id_map, specs::Vector{Pair{EagerTaskSpec,EagerThunk}})
    return Base.mapany(specs) do spec
        eager_process_args_submission_to_local(id_map, spec)
    end
end
function eager_process_options_submission_to_local(id_map, options::NamedTuple)
    @nospecialize options
    if haskey(options, :syncdeps)
        raw_syncdeps = options.syncdeps
        syncdeps = Set{Any}()
        for raw_dep in raw_syncdeps
            push!(syncdeps, eager_process_elem_submission_to_local(id_map, raw_dep))
        end
        return merge(options, (;syncdeps))
    else
        return options
    end
end
function eager_spawn(spec::EagerTaskSpec)
    Dagger.Sch.init_eager()

    # Generate new EagerThunk
    uid = eager_next_id()
    future = ThunkFuture()
    finalizer_ref = poolset(EagerThunkFinalizer(uid); device=MemPool.CPURAMDevice())

    # Return unlaunched EagerThunk
    return EagerThunk(uid, future, finalizer_ref)
end
function eager_launch!((spec, task)::Pair{EagerTaskSpec,EagerThunk})
    # Lookup EagerThunk -> ThunkID
    local args, options
    lock(Sch.EAGER_ID_MAP) do id_map
        args = eager_process_args_submission_to_local(id_map, spec=>task)
        options = eager_process_options_submission_to_local(id_map, spec.options)
    end

    # Submit the task
    thunk_id = eager_submit!(1,
                             task.uid, task.future, task.finalizer_ref,
                             spec.f, args, options)
    task.thunk_ref = thunk_id.ref
end
function eager_launch!(specs::Vector{Pair{EagerTaskSpec,EagerThunk}})
    ntasks = length(specs)

    uids = [task.uid for (_, task) in specs]
    futures = [task.future for (_, task) in specs]
    finalizer_refs = [task.finalizer_ref for (_, task) in specs]

    # Get all functions, args/kwargs, and options
    all_fs = Any[spec.f for (spec, _) in specs]
    all_args = lock(Sch.EAGER_ID_MAP) do id_map
        # Lookup EagerThunk -> ThunkID
        eager_process_args_submission_to_local(id_map, specs)
    end
    all_options = Any[spec.options for (spec, _) in specs]

    # Submit the tasks
    thunk_ids = eager_submit!(ntasks, uids, futures, finalizer_refs, all_fs, all_args, all_options)
    for i in 1:ntasks
        task = specs[i][2]
        task.thunk_ref = thunk_ids[i].ref
    end
end
